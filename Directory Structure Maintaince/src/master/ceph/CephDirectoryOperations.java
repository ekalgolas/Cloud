package master.ceph;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;

import master.metadata.Inode;
import master.metadata.MetaDataServerInfo;
import commons.AppConfig;
import commons.CommandsSupported;
import commons.Globals;
import commons.Message;
import commons.OutputFormatter;
import commons.dir.Directory;
import commons.dir.ICommandOperations;

public class CephDirectoryOperations implements ICommandOperations {
	/**
	 * Performs a tree search from the {@literal root} on the directory structure corresponding to the {@literal filePath}
	 *
	 * @param root
	 *            Root of directory structure
	 * @param filePath
	 *            Path to search
	 * @return Node corresponding to the path, null if not found
	 */
	private Directory search(Directory root, final String filePath, final StringBuffer resultCode) {
		// Get list of paths
		final String[] paths = filePath.split("/");
		int countLevel = 0;

		// Find the directory in directory tree
		for (final String path : paths) {
			// Match the root
			boolean found = false;
			if (root.getName()
					.equalsIgnoreCase(path)) {
				found = true;
				countLevel++;
			}

			// Check if the path corresponds to any child in this directory
			for (final Directory child : root.getChildren()) {
				if (child.getName()
						.equalsIgnoreCase(path)) {
					root = child;
					found = true;
					break;
				}
			}

			// If child was not found, path does not exists
			if (!found) {
				if(countLevel > 0) {
					resultCode.append(Globals.PARTIAL_PATH_FOUND);
				} else {
					resultCode.append(Globals.PATH_NOT_FOUND);
				}
				return root;
			}
		}

		// Return the node where the path was found
		resultCode.append(Globals.PATH_FOUND);
		return root;
	}

	/**
	 * Get the MDS server info to forward the command (read/write) to the respective MDS.
	 * @param inode
	 * @param isWrite
	 * @return MDS information
	 */
	private MetaDataServerInfo getRequiredMdsInfo(final Inode inode,final boolean isWrite)
	{
		MetaDataServerInfo serverInfo = null;
		for(final MetaDataServerInfo info:inode.getDataServerInfo())
		{
			if(Globals.ALIVE_STATUS.equalsIgnoreCase(info.getStatus()) &&
					(isWrite && Globals.PRIMARY_MDS.equals(info.getServerType()) ||
							!isWrite))
			{
				serverInfo = info;
				return serverInfo;
			}
		}
		return serverInfo;
	}

	/**
	 * Execute the command in the remote MDS server and fetch the processed message.
	 * 
	 * @param mkdir
	 * @param inode
	 * @param filePath
	 * @param isWrite
	 * @return Message containing the result.
	 */
	private Message remoteExecCommand(final CommandsSupported mkdir,
			final Inode inode,
			final String filePath,
			final boolean isWrite )
	{
		MetaDataServerInfo serverInfo = getRequiredMdsInfo(inode,isWrite);
		if(serverInfo == null) {
			return null;
		}
		while(serverInfo != null)
		{
			try
			{
				final Socket socket = new Socket(serverInfo.getIpAddress(), Integer.parseInt(AppConfig.getValue("client.masterPort")));
				final ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				final ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(new Message(mkdir + " " + filePath));
				outputStream.flush();

				// Wait and read the reply
				final Message message = (Message) inputStream.readObject();
				final String reply = message.getContent();
				System.out.println(reply);
				socket.close();
				return message;
			}
			catch(final UnknownHostException unkhostexp)
			{
				serverInfo.setStatus(Globals.DEAD_STATUS);
				System.err.println("Error occured while executing commands");
				unkhostexp.printStackTrace();
			}
			catch(final IOException ioexp)
			{
				serverInfo.setStatus(Globals.DEAD_STATUS);
				System.err.println("Error occured while executing commands");
				ioexp.printStackTrace();
			}
			catch (final ClassNotFoundException cnfexp)
			{
				serverInfo.setStatus(Globals.DEAD_STATUS);
				System.err.println("Error occured while executing commands");
				cnfexp.printStackTrace();
			}
			serverInfo = getRequiredMdsInfo(inode,isWrite);
		}

		return null;
	}

	@Override
	public Message ls(final Directory root, final String filePath, final String... arguments) throws InvalidPropertiesFormatException {
		final StringBuffer resultCode = new StringBuffer();
		String searchablePath;
		if(arguments != null && arguments.length > 0 && !"/".equals(arguments[0]))
		{
			searchablePath = filePath.substring(arguments[0].length());
		}
		else
		{
			searchablePath = filePath;
		}
		final Directory node = search(root,searchablePath,resultCode);

		if(node != null)
		{
			final Inode inode = node.getInode();
			if(inode.getInodeNumber() != null && Globals.PATH_FOUND.equals(resultCode))
			{
				final OutputFormatter output = new OutputFormatter();
				if(!node.isFile())
				{
					// If we reach here, it means valid directory was found
					// Compute output
					output.addRow("TYPE", "NAME");

					// Append children
					for (final Directory child : node.getChildren()) {
						final String type = child.isFile() ? "File" : "Directory";
						output.addRow(type, child.getName());
					}
				}
				else
				{
					output.addRow("TYPE", "NAME");
					output.addRow("File", node.getName());
				}
				final Message result = new Message(output.toString(), node.getInode().getDataServerInfo().toString());
				return result;
			}
			else if(inode.getInodeNumber() == null && Globals.PARTIAL_PATH_FOUND.equals(resultCode))
			{
				if(inode.getDataServerInfo() != null && inode.getDataServerInfo().size() > 0)
				{
					final Message message = remoteExecCommand(CommandsSupported.LS, inode, filePath, false);
					if(message != null) {
						return message;
					}
				}
			}
			else if(inode.getInodeNumber() != null)
			{
				return new Message(filePath+" is in an instable state");
			}
			else
			{
				return new Message(filePath+" Does not exist");
			}
		}
		else
		{
			return new Message(filePath+" Does not exist");
		}
		return null;
	}

	/**
	 * Create a resource in the directory tree
	 *
	 * @param root
	 *            Root of the directory structure to search in
	 * @param path
	 *            Path of the parent directory where the resource needs to be created
	 * @param name
	 *            Name of the resource
	 * @param isFile
	 *            Will create file if true, directory otherwise
	 * @throws InvalidPathException
	 */
	private void create(final Directory root, final String currentPath, final String name, final boolean isFile, final String fullPath) throws InvalidPathException {
		// Search and get to the directory where we have to create
		final StringBuffer resultCode = new StringBuffer();
		final Directory directory = search(root, currentPath, resultCode);

		if(directory != null)
		{
			final Inode inode = directory.getInode();
			if(inode.getInodeNumber() != null && Globals.PATH_FOUND.equals(resultCode))
			{
				if(!directory.isFile())
				{
					// Add file if isFile is true
					if (isFile) {
						final Directory file = new Directory(name, isFile, null);
						directory.getChildren()
						.add(file);
					} else {
						// Else, add directory here
						final Directory dir = new Directory(name, isFile, new ArrayList<Directory>());
						directory.getChildren()
						.add(dir);
					}
				}
				else
				{
					return; //need to add message explaining the path is not a directory.
				}
			}
			else if(inode.getInodeNumber() == null && Globals.PARTIAL_PATH_FOUND.equals(resultCode))
			{
				if(inode.getDataServerInfo() != null && inode.getDataServerInfo().size() > 0)
				{
					remoteExecCommand(CommandsSupported.MKDIR, inode, fullPath, false);
				}
			}
			else if(inode.getInodeNumber() != null)
			{
				return; // need to add message explaining the unstable state of master.metadata.
			}
			else
			{
				return; // need to add message explaining the path not found issue.
			}
		}
		else
		{
			return; // need to add message explaining the path not found issue.
		}
	}

	@Override
	public void mkdir(final Directory root, final String path, final String... arguments) throws InvalidPropertiesFormatException {
		// Get the parent directory and the name of directory
		String searchablePath;
		if(arguments != null && arguments.length > 0 && !"/".equals(arguments[0]))
		{
			searchablePath = path.substring(arguments[0].length());
		}
		else
		{
			searchablePath = path;
		}
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 2];
		final String dirPath = path.substring(0, searchablePath.length() - name.length() - 1);

		// Create the directory
		create(root, dirPath, name, false, path);

	}

	@Override
	public void touch(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rmdir(final Directory root, final String path, final String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rm(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public Message cd(final Directory root, final String filePath) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

}
