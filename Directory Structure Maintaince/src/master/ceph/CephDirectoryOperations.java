package master.ceph;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.InvalidPathException;
import java.util.InvalidPropertiesFormatException;

import commons.AppConfig;
import commons.Globals;
import commons.ICommandOperations;
import commons.Message;
import commons.OutputFormatter;
import metadata.Directory;
import metadata.Inode;
import metadata.MetaDataServerInfo;

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
	private Directory search(Directory root, final String filePath) {
		// Get list of paths
		final String[] paths = filePath.split("/");

		// Find the directory in directory tree
		for (final String path : paths) {
			// Match the root
			boolean found = false;
			if (root.getName()
					.equalsIgnoreCase(path)) {
				found = true;
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
				return null;
			}
		}

		// Return the node where the path was found
		return root;
	}
	
	/**
	 * Find the closest Directory that matches with the required file path.
	 * @param filePath
	 * @return closest directory.
	 */
	private Directory findClosestNode(String filePath)
	{
		int maxLevel = 0;
		String maxMatchPath = "";
		for(String node:Globals.subTreePartitionList.keySet())
		{
			int currentLevel = 0;
			int i=0;
			while(i< node.length() && i < filePath.length())
			{
				if(node.charAt(i) == filePath.charAt(i))
				{
					if(node.charAt(i) == '/')
						currentLevel++;
				}
				else
					break;
				i++;				
			}
			if(currentLevel > maxLevel && (i==node.length()))
			{
				maxLevel = currentLevel;
				maxMatchPath = node;
			}
		}
		return Globals.subTreePartitionList.get(maxMatchPath);
	}
	
	/**
	 * Get the MDS server info to forward the command (read/write) to the respective MDS. 
	 * @param closestNode
	 * @param isWrite
	 * @return MDS information
	 */
	private MetaDataServerInfo getRequiredMdsInfo(Directory closestNode,boolean isWrite)
	{
		MetaDataServerInfo serverInfo = null;
		for(MetaDataServerInfo info:closestNode.getInode().getDataServerInfo())
		{
			if(Globals.ALIVE_STATUS.equalsIgnoreCase(info.getStatus()) && 
				((isWrite && Globals.PRIMARY_MDS.equals(info.getServerType())) ||
				(!isWrite)))
			{
				serverInfo = info;
				return serverInfo;
			}
		}
		return serverInfo;
	}
	
	/**
	 * Execute the command in the remote MDS server and fetch the processed message.
	 * @param command
	 * @param closestNode
	 * @param filePath
	 * @param isWrite
	 * @return Message containing the result.
	 */
	private Message remoteExecCommand(String command,
									  Directory closestNode, 
									  String filePath,
									  boolean isWrite )
	{
		MetaDataServerInfo serverInfo = getRequiredMdsInfo(closestNode,isWrite);
		if(serverInfo == null)
			return null;
		while(serverInfo != null)
		{
			try
			{
				final Socket socket = new Socket(serverInfo.getIpAddress(), Integer.parseInt(AppConfig.getValue("client.masterPort")));
				final ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				final ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(new Message(command+" "+filePath));
				outputStream.flush();
	
				// Wait and read the reply
				final Message message = (Message) inputStream.readObject();
				final String reply = message.getContent();
				System.out.println(reply);
				return message;
			}
			catch(UnknownHostException unkhostexp)
			{
				serverInfo.setStatus(Globals.DEAD_STATUS);
				System.err.println("Error occured while executing commands");
				unkhostexp.printStackTrace();
			}
			catch(IOException ioexp)
			{
				serverInfo.setStatus(Globals.DEAD_STATUS);
				System.err.println("Error occured while executing commands");
				ioexp.printStackTrace();
			}
			catch (ClassNotFoundException cnfexp) 
			{
				serverInfo.setStatus(Globals.DEAD_STATUS);
				System.err.println("Error occured while executing commands");
				cnfexp.printStackTrace();
			}
			serverInfo = getRequiredMdsInfo(closestNode,isWrite);
		}
		
		return null;
	}
	
	@Override
	public Message ls(Directory root, String filePath, String... arguments) throws InvalidPropertiesFormatException {
		Directory node = search(root,filePath);
			
		if(node != null)
		{
			Inode inode = node.getInode();
			if(inode.getInodeNumber() != null)
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
				Message result = new Message(output.toString(), node.getInode().getDataServerInfo().toString());
				return result;
			}
			else
			{
				if(inode.getDataServerInfo() != null && inode.getDataServerInfo().size() > 0)
				{
					Directory closestNode = findClosestNode(filePath);
					Message message = remoteExecCommand(Globals.LS,closestNode,filePath,false);
					if(message != null)
						return message;
				}
			}
		}
		else
		{
			new Message(filePath+" Does not exist");
		}
		return null;
	}

	@Override
	public void mkdir(Directory root, String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void touch(Directory root, String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rmdir(Directory root, String path, String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rm(Directory root, String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public Message cd(Directory root, String filePath) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

}
