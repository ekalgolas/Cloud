package master.ceph;

import java.awt.image.ReplicateScaleFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.Date;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import com.sun.xml.internal.ws.api.message.MessageMetadata;

import commons.AppConfig;
import commons.CommandsSupported;
import commons.CompletionStatusCode;
import commons.Globals;
import commons.Message;
import commons.OutputFormatter;
import commons.dir.Directory;
import commons.dir.ICommandOperations;
import master.Master;
import master.metadata.Inode;
import master.metadata.MetaDataServerInfo;

public class CephDirectoryOperations implements ICommandOperations {
	/**
	 * Performs a tree search from the {@literal root} on the directory
	 * structure corresponding to the {@literal filePath}
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
					long operationCounter = child.getOperationCounter();
					operationCounter++;
					child.setOperationCounter(operationCounter);
					break;
				}
			}

			// If child was not found, path does not exists
			if (!found) {
				if (countLevel > 0) {
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
	 * Get the MDS server info to forward the command (read/write) to the
	 * respective MDS.
	 * 
	 * @param inode
	 * @param isWrite
	 * @return MDS information
	 */
	private MetaDataServerInfo getRequiredMdsInfo(final Inode inode, final boolean isWrite) {
		MetaDataServerInfo serverInfo = null;
		for (final MetaDataServerInfo info : inode.getDataServerInfo()) 
		{
			if (Globals.ALIVE_STATUS.equalsIgnoreCase(info.getStatus()) && 
					(isWrite && Globals.PRIMARY_MDS.equals(info.getServerType()) || 
							!isWrite)) {
				serverInfo = info;
				return serverInfo;
			}
		}
		return serverInfo;
	}

	/**
	 * Execute the command in the remote MDS server and fetch the processed
	 * message.
	 * 
	 * @param command
	 * @param filePath
	 * @param mdsServer
	 * @return Message containing the result.
	 */
	private Message remoteExecCommand(final CommandsSupported command, 
			final String filePath,
			final MetaDataServerInfo mdsServer,
			boolean primaryMessage) 
	{
		if (mdsServer != null) {
			try 
			{
				final Socket socket = new Socket(mdsServer.getIpAddress(),
						Integer.parseInt(AppConfig.getValue(Globals.CLIENT_MDS_MASTER_PORT)));
				final ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				final ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(new Message(command + " " + filePath,primaryMessage?Globals.PRIMARY_MDS:null));
				outputStream.flush();

				// Wait and read the reply
				final Message message = (Message) inputStream.readObject();				
				final String reply = message.getContent();
				System.out.println(reply);
				socket.close();
				return message;
			} 
			catch (final UnknownHostException unkhostexp) 
			{
				mdsServer.setStatus(Globals.DEAD_STATUS);
				System.err.println("Error occured while executing commands");
				unkhostexp.printStackTrace();
			} 
			catch (final IOException ioexp) 
			{
				mdsServer.setStatus(Globals.DEAD_STATUS);
				System.err.println("Error occured while executing commands");
				ioexp.printStackTrace();
			} 
			catch (final ClassNotFoundException cnfexp) 
			{
				mdsServer.setStatus(Globals.DEAD_STATUS);
				System.err.println("Error occured while executing commands");
				cnfexp.printStackTrace();
			}
		}

		return null;
	}

	@Override
	public Message ls(final Directory root, final String filePath, final String... arguments)
			throws InvalidPropertiesFormatException {
		final StringBuffer resultCode = new StringBuffer();
		String searchablePath;
		if (arguments != null && arguments.length > 0 && !"/".equals(arguments[0])) {
			searchablePath = filePath.substring(arguments[0].length());
		} else {
			searchablePath = filePath;
		}
		final Directory node = search(root, searchablePath, resultCode);

		if (node != null) 
		{
			final Inode inode = node.getInode();
			if (inode.getInodeNumber() != null && Globals.PATH_FOUND.equals(resultCode)) 
			{
				final OutputFormatter output = new OutputFormatter();
				if (!node.isFile()) 
				{
					// If we reach here, it means valid directory was found
					// Compute output
					output.addRow("TYPE", "NAME");

					// Append children
					for (final Directory child : node.getChildren()) 
					{
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
			else if (inode.getInodeNumber() == null && Globals.PARTIAL_PATH_FOUND.equals(resultCode)) 
			{
				if (inode.getDataServerInfo() != null && inode.getDataServerInfo().size() > 0) 
				{
					final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, false); 
					final Message message = remoteExecCommand(CommandsSupported.LS, filePath, mdsServer, false);
					if (message != null) 
					{
						return message;
					}
				}
			} 
			else if (inode.getInodeNumber() != null) 
			{
				return new Message(filePath + " is in an instable state");
			} 
			else 
			{
				return new Message(filePath + " Does not exist");
			}
		} 
		else 
		{
			return new Message(filePath + " Does not exist");
		}
		return null;
	}
	
	private Message updateReplicas(final CommandsSupported command,
			Inode inode,
			final String fullPath)
	{
		boolean finalStatus = true;
		final StringBuilder errorMessages = new StringBuilder(); 
		if(inode != null && 
				inode.getDataServerInfo() != null && 
				inode.getDataServerInfo().size() > 0)
		{
			for(MetaDataServerInfo metaData:inode.getDataServerInfo())
			{
				if(Globals.REPLICA_MDS.equals(metaData.getServerType()))
				{
					Message replicaMessage = remoteExecCommand(command, fullPath, metaData, true);
					if(replicaMessage != null)
					{
						finalStatus &= (CompletionStatusCode.SUCCESS.name()
										.equals(replicaMessage.getCompletionCode()));
						if(!CompletionStatusCode.SUCCESS.name()
								.equals(replicaMessage.getCompletionCode()))
						{
							errorMessages.append(replicaMessage.getContent()+"\n");
						}
					}
					else
					{
						finalStatus &= false;
					}
				}
			}
		}
		if(finalStatus)
		{
			return new Message(fullPath+" created Successfully",
						"",
						CompletionStatusCode.SUCCESS.name());
		}
		Message finalMessage = new Message(errorMessages.toString(),"",CompletionStatusCode.ERROR.name()); 
		return finalMessage;
	}

	/**
	 * Create a resource in the directory tree
	 *
	 * @param root
	 *            Root of the directory structure to search in
	 * @param path
	 *            Path of the parent directory where the resource needs to be
	 *            created
	 * @param name
	 *            Name of the resource
	 * @param isFile
	 *            Will create file if true, directory otherwise
	 * @throws InvalidPathException
	 */
	private Message create(final Directory root, final String currentPath, 
			final String name, 
			final boolean isFile,
			final String fullPath,
			boolean primaryMessage) throws InvalidPathException 
	{
		// Search and get to the directory where we have to create
		final StringBuffer resultCode = new StringBuffer();
		final Directory directory = search(root, currentPath, resultCode);

		if (directory != null) 
		{
			final Inode inode = directory.getInode();
			if (inode.getInodeNumber() != null && Globals.PATH_FOUND.equals(resultCode)) 
			{
				try
				{
					MetaDataServerInfo serverInfo = getRequiredMdsInfo(inode, true);
					if((primaryMessage) || (serverInfo != null && 
							InetAddress.getLocalHost().equals(serverInfo.getIpAddress())))
					{
						if (!directory.isFile()) 
						{
							// Add file if isFile is true
							if (isFile) 
							{
								boolean fileExist = false;
								for (Directory child : directory.getChildren()) 
								{
									if (child.getName().equalsIgnoreCase(name) && child.isFile()) 
									{
										fileExist = true;
										break;
									}
								}
								if (fileExist) 
								{
									return new Message("File already exists",
														"",
														CompletionStatusCode.FILE_EXISTS.name());
								}
								final Directory file = new Directory(name, isFile, null);
								Long newInodeNumber = Master.getInodeNumber();
								if(newInodeNumber != -1) // When inode number available
								{
									Inode newInode = new Inode();
									newInode.setInodeNumber(newInodeNumber);
									newInode.getDataServerInfo().addAll(inode.getDataServerInfo());
									file.setInode(newInode);
									directory.getChildren().add(file);
									Message replicaMessages = updateReplicas(CommandsSupported.MKDIR,inode,fullPath);
									if(CompletionStatusCode.SUCCESS.name().equals(replicaMessages.getCompletionCode()))
									{
										return new Message(file.getName()+" created succesfully",
												directory.getInode().getDataServerInfo().toString(),
												CompletionStatusCode.SUCCESS.name());
									}															
									return new Message(replicaMessages.getContent(),
											directory.getInode().getDataServerInfo().toString(),
											CompletionStatusCode.ERROR.name());
								}
								return new Message("Inode Number Exhausted",
										directory.getInode().getDataServerInfo().toString(),
										CompletionStatusCode.ERROR.name());
							} 
							else 
							{
								// Else, add directory here
								boolean fileExist = false;
								for (Directory child : directory.getChildren()) 
								{
									if (child.getName().equalsIgnoreCase(name) && !child.isFile()) 
									{
										fileExist = true;
										break;
									}
								}
								if (fileExist) 
								{
									return new Message("Directory already exists",
											"",
											CompletionStatusCode.DIR_EXISTS.name());
								}
								final Directory dir = new Directory(name, isFile, new ArrayList<Directory>());
								directory.getChildren().add(dir);
								Long newInodeNumber = Master.getInodeNumber();
								if(newInodeNumber != -1)// When inode number available
								{
									Inode newInode = new Inode();
									newInode.setInodeNumber(newInodeNumber);
									newInode.getDataServerInfo().addAll(inode.getDataServerInfo());
									dir.setInode(newInode);
									Message replicaMessages = updateReplicas(CommandsSupported.MKDIR,inode,fullPath);
									if(CompletionStatusCode.SUCCESS.name()
											.equals(replicaMessages.getCompletionCode()))
									{
										return new Message(dir.getName()+" created succesfully",
												directory.getInode().getDataServerInfo().toString(),
												CompletionStatusCode.SUCCESS.name());
									}
									return new Message(replicaMessages.getContent(),
											directory.getInode().getDataServerInfo().toString(),
											CompletionStatusCode.ERROR.name());
								}
								return new Message("Inode Number Exhausted",
										directory.getInode().getDataServerInfo().toString(),
										CompletionStatusCode.ERROR.name());
							}							
						} 
						else 
						{
							return new Message("Directory expected",
									"",
									CompletionStatusCode.DIR_EXPECTED.name());
						}
					}
					else
					{
						return remoteExecCommand(CommandsSupported.MKDIR, 
												fullPath, 
												serverInfo,
												false);
					}
				}
				catch(UnknownHostException unexp)
				{
					return new Message(unexp.getLocalizedMessage(),
							"",
							CompletionStatusCode.ERROR.name());
				}
			} 
			else if (inode.getInodeNumber() == null && Globals.PARTIAL_PATH_FOUND.equals(resultCode)) 
			{
				if (inode.getDataServerInfo() != null && 
						inode.getDataServerInfo().size() > 0) 
				{
					final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 
					return remoteExecCommand(CommandsSupported.MKDIR, fullPath, mdsServer, false);
				}
			} 
			else if (inode.getInodeNumber() != null) 
			{
				return new Message("MetaData in unstable state",
						"",
						CompletionStatusCode.UNSTABLE.name()); // need to add message explaining the unstable state of
						// metadata.
			} 
			else 
			{
				return new Message("Path"+ fullPath +" not found",
						"",
						CompletionStatusCode.NOT_FOUND.name());
						// issue.
			}
		} 		
		return new Message("Path"+ fullPath +" not found",
				"",
				CompletionStatusCode.NOT_FOUND.name());
	}

	@Override
	public Message mkdir(final Directory root, 
			final String path, 
			final String... arguments)
			throws InvalidPropertiesFormatException 
	{
		// Get the parent directory and the name of directory
		String searchablePath;
		if (arguments != null && arguments.length > 0 && !"/".equals(arguments[0])) 
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
		
		boolean primaryMessage = false;
		if(arguments != null && arguments.length >= 2)
		{
			primaryMessage = Globals.PRIMARY_MDS.equals(arguments[1]);
		}

		// Create the directory
		return create(root, dirPath, name, false, path,primaryMessage);
	}

	@Override
	public Message touch(final Directory root, 
			final String path,
			String... arguments) 
			throws InvalidPropertiesFormatException 
	{
		// Get the parent directory and the name of directory
		String searchablePath;
		if (arguments != null && arguments.length > 0 && !"/".equals(arguments[0])) 
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

		StringBuffer resultCode = new StringBuffer();
		final Directory directory = search(root, dirPath, resultCode);
		if(directory != null)
		{
			Inode inode = directory.getInode();
			if(inode.getInodeNumber() == null && Globals.PARTIAL_PATH_FOUND.equals(resultCode))
			{
				if (inode.getDataServerInfo() != null && 
						inode.getDataServerInfo().size() > 0) 
				{
					final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 
					return remoteExecCommand(CommandsSupported.TOUCH, path, mdsServer, false);
				}
			}
			else if(inode.getInodeNumber() != null && Globals.PATH_FOUND.equals(resultCode))
			{
				// Create the file
				final Directory file = new Directory(name, true, null);
				final List<Directory> contents = directory.getChildren();
				boolean found = false;
				Directory touchFile = null; 
				for (final Directory child : contents) 
				{
					if (child.equals(file)) 
					{
						// Already present, set modified timestamp to current
						touchFile = child;
						found = true;
						break;
					}
				}
				if(found)
				{
					MetaDataServerInfo metaData = getRequiredMdsInfo(inode, true);
					try
					{
						if(metaData != null && 
								InetAddress.getLocalHost().equals(metaData.getIpAddress()))
						{
							if(touchFile != null)
							{
								touchFile.setModifiedTimeStamp(new Date().getTime());
								if(inode.getDataServerInfo() != null &&
										inode.getDataServerInfo().size()>1)
								{
									Message finalMessage = updateReplicas(CommandsSupported.TOUCH, 
											inode, path);
									if(finalMessage != null && 
											CompletionStatusCode.SUCCESS.name()
											.equals(finalMessage.getCompletionCode()))
									{
										return new Message("Touch successful",
												inode.getDataServerInfo().toString(),
												CompletionStatusCode.SUCCESS.name()
												);
									}
									else
									{
										return new Message(finalMessage.getContent(),
												inode.getDataServerInfo().toString(),
												CompletionStatusCode.ERROR.name()
												);
									}
								}
								else
									return new Message("Touch successful",
											inode.getDataServerInfo().toString(),
											CompletionStatusCode.SUCCESS.name()
											);
							}
							else
							{
								return new Message("System in unstable state",
										inode.getDataServerInfo().toString(),
										CompletionStatusCode.UNSTABLE.name()
										);
							}
						}
						else
						{							
							return remoteExecCommand(CommandsSupported.TOUCH, 
									path, metaData, false);
						}
					}
					catch(UnknownHostException unexp)
					{
						return new Message(unexp.getLocalizedMessage(),
								inode.getDataServerInfo().toString(),
								CompletionStatusCode.ERROR.name()
								);
					}
				}
			}
			else if (inode.getInodeNumber() != null) 
			{
				return new Message("MetaData in unstable state",
						"",
						CompletionStatusCode.UNSTABLE.name()); // need to add message explaining the unstable state of
						// metadata.
			} 
			else 
			{
				return new Message("Path"+ path +" not found",
						"",
						CompletionStatusCode.NOT_FOUND.name());
						// issue.
			}
			
		}

		// Create the file
		final Directory file = new Directory(name, true, null);
		final List<Directory> contents = directory.getChildren();
		boolean found = false;
		for (final Directory child : contents) 
		{
			if (child.equals(file)) 
			{
				// Already present, set modified timestamp to current
				child.setModifiedTimeStamp(new Date().getTime());
				found = true;
				break;
			}
		}
		if (!found) 
		{
			// Not present, add it in the list
			file.setModifiedTimeStamp(new Date().getTime());
			contents.add(file);
		}
		directory.setChildren(contents);
		return null;
	}

	@Override
	public void rmdir(final Directory root, final String path, final String... arguments)
			throws InvalidPropertiesFormatException {
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