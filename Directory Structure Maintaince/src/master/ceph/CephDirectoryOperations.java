package master.ceph;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

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
		final StringBuffer filePathBuf; 
		if(filePath.charAt(0) == '/')
		{
			filePathBuf = new StringBuffer(filePath.substring(1));
		}
		else
		{
			filePathBuf = new StringBuffer(filePath);
		}
		// Get list of paths
		final String[] paths = filePathBuf.toString().split("/");
		int countLevel = 0;
//		System.out.println("filepath:"+filePath);
		// Find the directory in directory tree
		for (final String path : paths) {
			// Match the root
			boolean found = false;
			if (root.getName()
					.equals(path)) {
//				System.out.println("matched node:"+root.getName());
				found = true;
				countLevel++;
			}
			
			if(root.getChildren() != null)
			{
				// Check if the path corresponds to any child in this directory
				for (final Directory child : root.getChildren()) {					
					if (child.getName()
							.equals(path)) {
//						System.out.println("Child Name:"+child.getName());
						root = child;
						found = true;					
						Long operationCounter = child.getOperationCounter();
						operationCounter++;
						child.setOperationCounter(operationCounter);
						break;
					}
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
			boolean primaryMessage,
			Long inodeNumber) 
	{
		System.out.println("Calling "+mdsServer);
		if (mdsServer != null) {
			try 
			{
				final Socket socket = new Socket(mdsServer.getIpAddress(),
						Integer.parseInt(AppConfig.getValue(Globals.MDS_SERVER_PORT)));
				final ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				final ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(new Message(command + " " + filePath,
						primaryMessage?(Globals.PRIMARY_MDS+":"+inodeNumber):""));
				outputStream.flush();

				// Wait and read the reply
				final Message message = (Message) inputStream.readObject();				
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
		try
		{
			final StringBuffer resultCode = new StringBuffer();
			//Contains the relative path for the current partition.
			String searchablePath;
			if (arguments != null && 
					arguments.length > 0 && 
					(!"/".equals(arguments[0]) && !"root".equals(arguments[0]))) {
				searchablePath = filePath.substring(arguments[0].length());
			} else {
				searchablePath = filePath;
			}
			final Directory node;
			
			// True if detailed output asked for LS command (LSL)
	        final boolean isDetailed = (arguments != null) 
	                && arguments[arguments.length - 1].equals("-l");
			
	        //If partial match for the command path is found, start searching from that node. 
	        //Else start from the root node.
			if(searchablePath != null && 
					!"/".equals(searchablePath.trim()) && 
					!"".equals(searchablePath.trim()))
			{
				node = search(root, searchablePath, resultCode);
			}
			else
			{
				node = root;
				resultCode.append(Globals.PATH_FOUND);
			}
	
			//If search returns a non null node.
			if (node != null) 
			{
				final Inode inode = node.getInode();
				String resultcodeValue = resultCode.toString().trim();
				//If path found in the current MDS 
				if (inode.getInodeNumber() != null && 
						Globals.PATH_FOUND.equals(resultcodeValue)) 
				{
					final OutputFormatter output = new OutputFormatter();
					//If the path is a directory
					if (!node.isFile()) 
					{
						// If we reach here, it means valid directory was found
						// Compute output
						if(isDetailed)
						{
							output.addRow("TYPE", "NAME", "SIZE", "TIMESTAMP");
						}
						else
						{
							output.addRow("TYPE", "NAME");
						}
	
						// Append children
						for (final Directory child : node.getChildren()) 
						{
							final String type = child.isFile() ? "File" : "Directory";
							if(isDetailed) {
							    output.addRow(type,
							            child.getName(),
							            child.getSize().toString(),
							            child.getModifiedTimeStamp().toString());
							}
							else
							{
								output.addRow(type, child.getName());
							}
						}
					} 
					else //If the path is a file
					{
						if(isDetailed)
						{
							output.addRow("TYPE", "NAME", "SIZE", "TIMESTAMP");
							output.addRow("File",
									node.getName(),
									node.getSize().toString(),
									node.getModifiedTimeStamp().toString());
						}
						else
						{
							output.addRow("TYPE", "NAME");
							output.addRow("File", node.getName());
						}
					}
					final Message result = new Message(output.toString(), 
							inode.getDataServerInfo().toString());
					return result;
				} 
				//If the path lead to another MDS. Forward the command to the resp MDS.
				else if (inode.getInodeNumber() == null && 
						(Globals.PARTIAL_PATH_FOUND.equals(resultcodeValue)||
								Globals.PATH_FOUND.equals(resultcodeValue))) 
				{
					if (inode.getDataServerInfo() != null && 
							inode.getDataServerInfo().size() > 0) 
					{
						final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 
						final CommandsSupported remoteCommand;
						if(isDetailed)
						{
							remoteCommand = CommandsSupported.LSL;
						}
						else
						{
							remoteCommand = CommandsSupported.LS;
						}
						final Message message = remoteExecCommand(remoteCommand, 
								filePath, mdsServer, false, null);
						if (message != null) 
						{
							return message;
						}
					}
				}
				//If partial path found but the inode indicates 
				//that the path existing in current MDS
				else if (inode.getInodeNumber() != null && 
						!Globals.PARTIAL_PATH_FOUND.equals(resultcodeValue)) 
				{
					return new Message(filePath + " is in an instable state");
				} 
				//If the path not found.
				else 
				{
					return new Message(filePath + " Does not exist");
				}
			} 
			//If the path not found.
			else 
			{
				return new Message(filePath + " Does not exist");
			}
		}
		catch(Exception exp)
		{
			return new Message(exp.getLocalizedMessage()+" error occurred",
					"",
					CompletionStatusCode.ERROR.name());
		}
		return new Message("LS command completed without any errors and proper output",
				"",
				CompletionStatusCode.ERROR.name());
	}
	
	private Message updateReplicas(final CommandsSupported command,
			final Inode inode,
			final String fullPath,
			final Long inodeNumber)
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
					Message replicaMessage = remoteExecCommand(command, 
							fullPath, metaData, true, inodeNumber);
					if(replicaMessage != null)
					{
						finalStatus &= (CompletionStatusCode.SUCCESS.name()
										.equals(replicaMessage.getCompletionCode()
												.toString().trim()));
						if(!CompletionStatusCode.SUCCESS.name()
								.equals(replicaMessage.getCompletionCode()
										.toString().trim()))
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
			return new Message(fullPath+ " " +command.name() +"  Successfully",
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
			boolean primaryMessage,
			Long inodeNumber) throws InvalidPathException 
	{
		// Search and get to the directory where we have to create
		final StringBuffer resultCode = new StringBuffer();
		final Directory directory;
		if(Globals.subTreePartitionList.containsKey(fullPath))
		{
			directory = Globals.subTreePartitionList.get(fullPath);
		}
		else
		{
			if(currentPath != null && 
					!"/".equals(currentPath.trim()) && 
					!"".equals(currentPath.trim()))
			{
				directory = search(root, currentPath, resultCode);
			}
			else
			{
				directory = root;
				resultCode.append(Globals.PATH_FOUND);
			}
		}
		
//		System.out.println("Create => "+directory);

		if (directory != null) 
		{
			final Inode inode = directory.getInode();
//			System.out.println("inode:"+inode);
//			System.out.println("resultCode:"+resultCode);
			String resultcodeValue = resultCode.toString().trim();
			if (inode.getInodeNumber() != null && 
					Globals.PATH_FOUND.equals(resultcodeValue)) 
			{
//				System.out.println("Inside Path Found");
				try
				{
					MetaDataServerInfo serverInfo = getRequiredMdsInfo(inode, true);
					if((primaryMessage) || (serverInfo != null && 
							Master.getMdsServerIpAddress().equals(serverInfo.getIpAddress())))
					{
						if (!directory.isFile()) 
						{
							final Directory node = new Directory(name, isFile, null);
							boolean fileExist = false;
							for (Directory child : directory.getChildren()) 
							{
								if (child.equals(node)) 
								{
									fileExist = true;
									break;
								}
							}
							if (fileExist) 
							{
								return new Message(node.getName()+ " already exists",
													"",
													(isFile)?
															CompletionStatusCode.FILE_EXISTS.name():
															CompletionStatusCode.DIR_EXISTS.name());
							}
							if(!isFile)
							{
								node.setChildren(new ArrayList<Directory>());
							}
							Long newInodeNumber;
							if(primaryMessage)
							{
								newInodeNumber = inodeNumber;
							}
							else
							{
								newInodeNumber = Master.getInodeNumber(); 
							}
							
							if(newInodeNumber != -1) // When inode number available
							{
								Inode newInode = new Inode();
								newInode.setInodeNumber(newInodeNumber);
								newInode.getDataServerInfo().addAll(inode.getDataServerInfo());
								node.setInode(newInode);
								directory.getChildren().add(node);
								if(primaryMessage)
								{
									return new Message(node.getName()+" created succesfully",
											directory.getInode().getDataServerInfo().toString(),
											CompletionStatusCode.SUCCESS.name());
								}
								Message replicaMessages = updateReplicas(CommandsSupported.MKDIR,
										inode,
										fullPath,
										newInodeNumber);
								if(CompletionStatusCode.SUCCESS.name()
										.equals(replicaMessages.getCompletionCode()
												.toString().trim()))
								{
									return new Message(node.getName()+" created succesfully",
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
												false,
												null);
					}
				}
				catch(Exception unexp)
				{
					return new Message(unexp.getLocalizedMessage(),
							"",
							CompletionStatusCode.ERROR.name());
				}
			} 
			else if (inode.getInodeNumber() == null && 
					(Globals.PARTIAL_PATH_FOUND.equals(resultcodeValue) || 
							Globals.PATH_FOUND.equals(resultcodeValue))) 
			{
//				System.out.println("Forwarding mkdir to another MDS");
				if (inode.getDataServerInfo() != null && 
						inode.getDataServerInfo().size() > 0) 
				{
					final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 
//					System.out.println("Metadata Server:"+mdsServer);
					return remoteExecCommand(CommandsSupported.MKDIR, 
							fullPath, mdsServer, false, null);
				}
			} 
			else if (inode.getInodeNumber() != null) 
			{
//				System.out.println("Inside Unstable");
				return new Message("MetaData in unstable state",
						"",
						CompletionStatusCode.UNSTABLE.name()); // need to add message explaining the unstable state of
						// metadata.
			} 
			else 
			{
//				System.out.println("Inside Path Not Found");
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
		System.out.println("Calling mkdir");
		// Get the parent directory and the name of directory
		String searchablePath;
		if (arguments != null && 
				arguments.length > 0 && 
				(!"/".equals(arguments[0]) && !"root".equals(arguments[0]))) 
		{
			searchablePath = path.substring(arguments[0].length());
		} 
		else 
		{
			searchablePath = path;
		}
//		System.out.println("searchablePath: "+searchablePath);
		final String[] paths = searchablePath.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = searchablePath.substring(0, searchablePath.length() - name.length() - 1);
		
//		System.out.println("dirPath: "+dirPath);
//		System.out.println("name: "+name);
		boolean primaryMessage = false;
		Long inodeNumber = null;
		if(arguments != null && arguments.length >= 2)
		{
			String[] primaryMessagesContent = arguments[1].split(":");
			primaryMessage = Globals.PRIMARY_MDS.equals(primaryMessagesContent[0].trim());
			if(primaryMessagesContent.length > 1)
			{
				inodeNumber = Long.valueOf(primaryMessagesContent[1].trim());
			}
		}
//		System.out.println("inodeNumber:"+ inodeNumber);

		// Create the directory
		return create(root, dirPath, name, false, path,primaryMessage,inodeNumber);
	}

	@Override
	public Message touch(final Directory root, 
			final String path,
			String... arguments) 
			throws InvalidPropertiesFormatException 
	{
		System.out.println("Calling touch");
		// Get the parent directory and the name of directory
		String searchablePath;
		if (arguments != null && 
				arguments.length > 0 && 
				(!"/".equals(arguments[0]) && !"root".equals(arguments[0]))) 
		{
			searchablePath = path.substring(arguments[0].length());
		} 
		else 
		{
			searchablePath = path;
		}
		final String[] paths = searchablePath.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = searchablePath.substring(0, searchablePath.length() - name.length() - 1);
		
		System.out.println("dirPath:"+dirPath);
		System.out.println("name:"+name);
		System.out.println(Arrays.toString(arguments));
		
		boolean primaryMessage = false;
		Long inodeNumber = null;
		if(arguments != null && arguments.length >= 2)
		{
			String[] primaryMessagesContent = arguments[1].split(":");
			primaryMessage = Globals.PRIMARY_MDS.equals(primaryMessagesContent[0].trim());
//			System.out.println("Crossed reading primary");
			System.out.println(Arrays.toString(primaryMessagesContent));
			if(primaryMessagesContent.length > 1 && 
					(primaryMessagesContent[1] != null && !"null".equals(primaryMessagesContent[1].trim())))
			{
//				System.out.println("Inside primary null");
				inodeNumber = Long.valueOf(primaryMessagesContent[1].trim());
			}
		}

		StringBuffer resultCode = new StringBuffer();
		final Directory directory;
		if(Globals.subTreePartitionList.containsKey(path))
		{
			System.out.println("getting from list");
			directory = Globals.subTreePartitionList.get(path);
		}
		else
		{
			if(dirPath != null && 
					!"/".equals(dirPath.trim()) && 
					!"".equals(dirPath.trim()))
			{
				System.out.println("Calling SEarch");
				directory = search(root, dirPath, resultCode);
				System.out.println("Search over");
			}
			else
			{
				System.out.println("Assigning Root");
				directory = root;
				resultCode.append(Globals.PATH_FOUND);
			}
		}
		if(directory != null)
		{
			Inode inode = directory.getInode();
			System.out.println("inode:"+inode);
			System.out.println("resultCode:"+resultCode);
//			System.out.println(inode.getInodeNumber() == null);
			String resultcodeValue = resultCode.toString().trim();
			if(inode.getInodeNumber() == null && 
					(Globals.PARTIAL_PATH_FOUND.equals(resultcodeValue) || 
							Globals.PATH_FOUND.equals(resultcodeValue)))
			{
//				System.out.println("Forwarding touch to another MDS");
				if (inode.getDataServerInfo() != null && 
						inode.getDataServerInfo().size() > 0) 
				{
					final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 
//					System.out.println("Metadata Server:"+mdsServer);
					return remoteExecCommand(CommandsSupported.TOUCH, path, mdsServer, false, null);
				}
			}
			else if(inode.getInodeNumber() != null && 
					Globals.PATH_FOUND.equals(resultcodeValue))
			{
//				System.out.println("Inside Path Found");
				// Create the file
				final Directory file = new Directory(name, true, null);
				final List<Directory> contents = directory.getChildren();
				boolean found = false;
				Directory touchFile = null; 
				if(directory.getName().equals(file.getName().trim()))
				{
					touchFile = directory;
					found = true;
				}
				else
				{
					for (final Directory child : contents) 
					{
						if (child.getName().equals(file.getName().trim())) 
						{
							// Already present, set modified timestamp to current
							touchFile = child;
							found = true;
							break;
						}
					}
				}
				if(found && touchFile != null)
				{
					MetaDataServerInfo metaData = getRequiredMdsInfo(touchFile.getInode(), true);
					if(touchFile.getInode().getInodeNumber() == null)
					{
						return remoteExecCommand(CommandsSupported.TOUCH, 
								path, metaData, false, null);
					}					
					try
					{
						if((primaryMessage) || (metaData != null && 
								Master.getMdsServerIpAddress().equals(metaData.getIpAddress())))
						{
							touchFile.setModifiedTimeStamp(new Date().getTime());
							Long operationcounter = touchFile.getOperationCounter();
							operationcounter++;
							touchFile.setOperationCounter(operationcounter);
							if((!primaryMessage) && inode.getDataServerInfo() != null &&
									inode.getDataServerInfo().size()>1)
							{
								Message finalMessage = updateReplicas(CommandsSupported.TOUCH, 
										inode, path, null);
								if(finalMessage != null && 
										CompletionStatusCode.SUCCESS.name()
										.equals(finalMessage.getCompletionCode()
												.toString().trim()))
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
							return remoteExecCommand(CommandsSupported.TOUCH, 
									path, metaData, false, null);
						}
					}
					catch(Exception unexp)
					{
						return new Message(unexp.getLocalizedMessage(),
								inode.getDataServerInfo().toString(),
								CompletionStatusCode.ERROR.name()
								);
					}
				}
				else
				{
					MetaDataServerInfo metaData = getRequiredMdsInfo(inode, true);
					try
					{
						if((primaryMessage) || (metaData != null && 
								Master.getMdsServerIpAddress()
								.equals(metaData.getIpAddress())))
						{
							Long newInodeNumber;
							if(primaryMessage)
							{
								newInodeNumber = inodeNumber;
							}
							else
							{
								newInodeNumber = Master.getInodeNumber();
							}
							if(newInodeNumber != -1)
							{
								Inode newInode = new Inode();
								newInode.setInodeNumber(newInodeNumber);
								newInode.getDataServerInfo().addAll(inode.getDataServerInfo());
								file.setInode(newInode);
								directory.getChildren().add(file);
								if(primaryMessage)
								{
									return new Message(file.getName()+" created succesfully",
											directory.getInode().getDataServerInfo().toString(),
											CompletionStatusCode.SUCCESS.name());
								}
								Message replicaMessages = updateReplicas(CommandsSupported.TOUCH,
										inode,path, newInodeNumber);
								if(CompletionStatusCode.SUCCESS.name()
										.equals(replicaMessages.getCompletionCode()
												.toString().trim()))
								{
									return new Message(file.getName()+" created succesfully",
											directory.getInode().getDataServerInfo().toString(),
											CompletionStatusCode.SUCCESS.name());
								}
								return new Message(replicaMessages.getContent(),
										directory.getInode().getDataServerInfo().toString(),
										CompletionStatusCode.ERROR.name()
										);
							}
							return new Message("Inode Number Exhausted",
									directory.getInode().getDataServerInfo().toString(),
									CompletionStatusCode.ERROR.name());
							
						}
						else
						{							
							return remoteExecCommand(CommandsSupported.TOUCH, 
									path, metaData, false, null);
						}
					}
					catch(Exception unexp)
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
//				System.out.println("Inside Unstable");
				return new Message("MetaData in unstable state",
						"",
						CompletionStatusCode.UNSTABLE.name()); // need to add message explaining the unstable state of
						// metadata.
			} 
			else 
			{
//				System.out.println("Inside Path Not Found");
				return new Message("Path"+ path +" not found",
						"",
						CompletionStatusCode.NOT_FOUND.name());
						// issue.
			}
			
		}
		return new Message("Touch Failed",
				"",
				CompletionStatusCode.ERROR.name());
	}
	
	private Message removeNode(final Directory root, 
			final String currentPath,
			final String name, 
			final boolean isFile,
			final String fullPath,
			boolean primaryMessage
			)
	{
		// Search and get to the directory where we have to create
		final StringBuffer resultCode = new StringBuffer();
		final Directory directory;
		System.out.println("Keys:"+Arrays.toString(Globals.subTreePartitionList.keySet().toArray()));
		System.out.println("fullPath:"+fullPath);
		
		if(Globals.subTreePartitionList.containsKey(fullPath))
		{
			directory = Globals.subTreePartitionList.get(fullPath);
			if(directory.isEmptyDirectory())
			{
				Inode inode = directory.getInode();
//				System.out.println("inode:"+inode);
//				System.out.println("resultCode:"+resultCode);
				MetaDataServerInfo metadata = getRequiredMdsInfo(inode, true);
				try
				{
					if((primaryMessage) || (metadata != null && 
							Master.getMdsServerIpAddress().equals(metadata.getIpAddress())))
					{
						Globals.subTreePartitionList.remove(fullPath);
						if(primaryMessage)
						{
							return new Message(name+" removed successfully",
									"",
									CompletionStatusCode.SUCCESS.name());
						}
						Message finalMessage = updateReplicas(CommandsSupported.RMDIR, 
								inode, fullPath, null);
						if(CompletionStatusCode.SUCCESS.name()
								.equals(finalMessage.getCompletionCode()
										.toString().trim()))
						{
							return new Message(name+" removed successfully",
									"",
									CompletionStatusCode.SUCCESS.name());
						}
						return new Message(finalMessage.getContent(),
								inode.getDataServerInfo().toString(),
								CompletionStatusCode.ERROR.name());
					}
					else
					{
						return remoteExecCommand(CommandsSupported.RMDIR, 
								fullPath, metadata, primaryMessage, null);
					}
				}
				catch(Exception unexp)
				{
					return new Message(unexp.getLocalizedMessage(),
							"",
							CompletionStatusCode.ERROR.name());
				}
			}
			return new Message(name+" not empty",
					directory.getInode().getDataServerInfo().toString(),
					CompletionStatusCode.NOT_EMPTY.name());
		}
		else
		{
			
			if(currentPath != null && 
					!"/".equals(currentPath.trim()) && 
					!"".equals(currentPath.trim()))
			{
				directory = search(root, currentPath, resultCode);
			}
			else
			{
				directory = root;
				resultCode.append(Globals.PATH_FOUND);
			}
			
			if(directory != null)
			{
				Inode inode = directory.getInode();
				System.out.println("inode:"+inode);
				System.out.println("resultCode:"+resultCode);
				
				String resultcodeValue = resultCode.toString().trim();
				
				if(inode.getInodeNumber() == null && 
						(Globals.PARTIAL_PATH_FOUND.equals(resultcodeValue) || 
								(Globals.PATH_FOUND.equals(resultcodeValue))))
				{
//					System.out.println("Forwarding rmdir to another MDS");
					if (inode.getDataServerInfo() != null && 
							inode.getDataServerInfo().size() > 0) 
					{
						final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 
//						System.out.println("Metadata Server:"+mdsServer);
						return remoteExecCommand(CommandsSupported.RMDIR, fullPath, mdsServer, false, null);
					}
				}
				else if(inode.getInodeNumber() != null && 
						Globals.PATH_FOUND.equals(resultcodeValue))
				{
//					System.out.println("Inside Path Found");
					Directory removeDirectory = null;
					boolean found = false;
					for(Directory child:directory.getChildren())
					{
						if(child.getName().equals(name) && !child.isFile())
						{
							removeDirectory = child;
							found = true;
							break;
						}
					}
					if(found && removeDirectory != null)
					{
						if(removeDirectory.isEmptyDirectory())
						{
							MetaDataServerInfo metaData = getRequiredMdsInfo(removeDirectory.getInode(), true);
							if(removeDirectory.getInode().getInodeNumber() == null)
							{
								return remoteExecCommand(CommandsSupported.RMDIR, 
										fullPath, metaData, false, null);
							}
							try
							{
								if((primaryMessage) ||(metaData != null &&
										Master.getMdsServerIpAddress()
										.equals(metaData.getIpAddress())))
								{
									directory.getChildren().remove(removeDirectory);
									if(primaryMessage)
									{
										return new Message(name+" removed successfully",
												"",
												CompletionStatusCode.SUCCESS.name());
									}
									Message finalMessage = updateReplicas(CommandsSupported.RMDIR, 
											removeDirectory.getInode(), fullPath, null);
									if(CompletionStatusCode.SUCCESS.name()
											.equals(finalMessage.getCompletionCode()
													.toString().trim()))
									{
										return new Message(name+" removed successfully",
												"",
												CompletionStatusCode.SUCCESS.name());
									}
									return new Message(finalMessage.getContent(),
											"",
											CompletionStatusCode.ERROR.name());
								}
								else
								{
									return remoteExecCommand(CommandsSupported.RMDIR, 
											fullPath, metaData, false, null);
								}
							}
							catch(Exception unexp)
							{
								return new Message(unexp.getLocalizedMessage(),
										"",
										CompletionStatusCode.ERROR.name());
							}
						}
						return new Message(name+" not empty",
								"",
								CompletionStatusCode.NOT_EMPTY.name());
					}
					return new Message(fullPath+" path not found",
							"",
							CompletionStatusCode.NOT_FOUND.name());
				}
				else if (inode.getInodeNumber() != null) 
				{
//					System.out.println("Inside Unstable");
					return new Message("MetaData in unstable state",
							"",
							CompletionStatusCode.UNSTABLE.name()); // need to add message explaining the unstable state of
							// metadata.
				} 
				else 
				{
//					System.out.println("Inside Path Not Found");
					return new Message("Path "+ fullPath +" not found",
							"",
							CompletionStatusCode.NOT_FOUND.name());
							// issue.
				}
			}
			else
			{
				return new Message(fullPath+ " not found",
						"",
						CompletionStatusCode.NOT_FOUND.name());
			}
		}
		
		return null;
	}

	@Override
	public Message rmdir(final Directory root, final String path, final String... arguments)
			throws InvalidPropertiesFormatException {
		System.out.println("Calling Rmdir");
		// Get the parent directory and the name of directory
		String searchablePath;
		if (arguments != null && 
				arguments.length > 0 && 
				(!"/".equals(arguments[0]) && !"root".equals(arguments[0]))) 
		{
			searchablePath = path.substring(arguments[0].length());
		} 
		else 
		{
			searchablePath = path;
		}		
		final String[] paths = searchablePath.split("/");
//		System.out.println("Searchable path:"+searchablePath);
//		System.out.println("paths:"+Arrays.toString(paths));
		final String name = paths[paths.length - 1];
		final String dirPath = searchablePath.substring(0, searchablePath.length() - name.length() - 1);
		
//		System.out.println("dirPath:"+dirPath);
//		System.out.println("name:"+name);
		
		boolean primaryMessage = false;
		if(arguments != null && arguments.length >= 2)
		{			
			String[] primaryMessagesContent = arguments[1].split(":");
			primaryMessage = Globals.PRIMARY_MDS.equals(primaryMessagesContent[0].trim());
		}
					
		return removeNode(root, dirPath, name, false, path, primaryMessage);
	}

	@Override
	public void rm(final Directory root, 
			final String path,
			String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public Message cd(final Directory root, 
			final String filePath,
			String... arguments) throws InvalidPropertiesFormatException {
		try
		{
			final StringBuffer resultCode = new StringBuffer();
			//Contains the relative path for the current partition.
			String searchablePath;
			if (arguments != null && 
					arguments.length > 0 && 
					(!"/".equals(arguments[0]) && !"root".equals(arguments[0]))) {
				searchablePath = filePath.substring(arguments[0].length());
			} else {
				searchablePath = filePath;
			}
			final Directory node;
					
	        //If partial match for the command path is found, start searching from that node. 
	        //Else start from the root node.
			if(searchablePath != null && 
					!"/".equals(searchablePath.trim()) && 
					!"".equals(searchablePath.trim()))
			{
				node = search(root, searchablePath, resultCode);
			}
			else
			{
				node = root;
				resultCode.append(Globals.PATH_FOUND);
			}
	
			//If search returns a non null node.
			if (node != null) 
			{
				final Inode inode = node.getInode();
				String resultcodeValue = resultCode.toString().trim();
				//If path found in the current MDS 
				if (inode.getInodeNumber() != null && 
						Globals.PATH_FOUND.equals(resultcodeValue)) 
				{					
					//If the path is a directory
					if (!node.isFile()) 
					{
						// If we reach here, it means valid directory was found
						final Message result = new Message("",
								inode.getDataServerInfo().toString(),
								CompletionStatusCode.SUCCESS.name());
						return result;
					} 
					else //If the path is a file
					{
						final Message result = new Message("Directory expected",
								inode.getDataServerInfo().toString(),
								CompletionStatusCode.DIR_EXPECTED.name());
						return result;
					}					
				} 
				//If the path lead to another MDS. Forward the command to the resp MDS.
				else if (inode.getInodeNumber() == null && 
						(Globals.PARTIAL_PATH_FOUND.equals(resultcodeValue)||
								Globals.PATH_FOUND.equals(resultcodeValue))) 
				{
					if (inode.getDataServerInfo() != null && 
							inode.getDataServerInfo().size() > 0) 
					{
						final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 						
						final Message message = remoteExecCommand(CommandsSupported.CD, 
								filePath, mdsServer, false, null);
						if (message != null) 
						{
							return message;
						}
					}
				}
				//If partial path found but the inode indicates 
				//that the path existing in current MDS
				else if (inode.getInodeNumber() != null && 
						!Globals.PARTIAL_PATH_FOUND.equals(resultcodeValue)) 
				{
					return new Message(filePath + " is in an instable state");
				} 
				//If the path not found.
				else 
				{
					return new Message(filePath + " Does not exist");
				}
			} 
			//If the path not found.
			else 
			{
				return new Message(filePath + " Does not exist");
			}
		}
		catch(Exception exp)
		{
			return new Message(exp.getLocalizedMessage()+" error occurred",
					"",
					CompletionStatusCode.ERROR.name());
		}
		return new Message("CD command completed without any errors and proper output",
				"",
				CompletionStatusCode.ERROR.name());
	}

    @Override
    public Directory releaseParentReadLocks(Directory root, String filePath) {
        // TODO Auto-generated method stub
        return null;
    }

}