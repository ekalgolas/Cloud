package master.ceph;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
	
	private final static Logger		LOGGER		= Logger.getLogger(CephDirectoryOperations.class);
	private final static HashMap<String,Socket> 					cachedSockets   = new HashMap<>();
	private final static HashMap<Socket,ObjectInputStream> 		cachedIpStreams = new HashMap<>();
	private final static HashMap<Socket,ObjectOutputStream> 		cachedOpStreams = new HashMap<>();
	
	public CephDirectoryOperations()
	{
		LOGGER.setLevel(Level.DEBUG);
	}
	
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
		// Find the directory in directory tree
		for (final String path : paths) {
			// Match the root
			boolean found = false;
			if (root.getName().equals(path)) {
				found = true;
				countLevel++;
			}
			
			if(root.getChildren() != null)
			{
				// Check if the path corresponds to any child in this directory
				for (final Directory child : root.getChildren()) {					
					if (child.getName()
							.equals(path)) {
						root = child;
						found = true;	
						countLevel++;
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
	private Message remoteExecCommand(final String command, 
			final String filePath,
			final MetaDataServerInfo mdsServer,
			final String messageHeader) 
	{
		if (mdsServer != null) 
		{
			try 
			{
				final Socket socket;
				final ObjectInputStream inputStream;
				final ObjectOutputStream outputStream;
				if(cachedSockets.containsKey(mdsServer.getServerName()))
				{
					socket = cachedSockets.get(mdsServer.getServerName());
					inputStream = cachedIpStreams.get(socket);
					outputStream = cachedOpStreams.get(socket);
				}
				else
				{
					socket = new Socket(mdsServer.getIpAddress(),
						Integer.parseInt(AppConfig.getValue(Globals.MDS_SERVER_PORT)));
					cachedSockets.put(mdsServer.getServerName(), socket);
					inputStream = new ObjectInputStream(socket.getInputStream());
					outputStream = new ObjectOutputStream(socket.getOutputStream());
					cachedIpStreams.put(socket, inputStream);
					cachedOpStreams.put(socket, outputStream);
				}
				outputStream.writeObject(new Message(command + " " + filePath,
									messageHeader));
				outputStream.flush();				

				// Wait and read the reply
				final Message message = (Message) inputStream.readObject();				
				return message;
			} 
			catch (final UnknownHostException unkhostexp) 
			{
				mdsServer.setStatus(Globals.DEAD_STATUS);
				LOGGER.error(unkhostexp.getLocalizedMessage());				
			} 
			catch (final IOException ioexp) 
			{
				mdsServer.setStatus(Globals.DEAD_STATUS);
				LOGGER.error(ioexp.getLocalizedMessage());							
			} 
			catch (final ClassNotFoundException cnfexp) 
			{
				mdsServer.setStatus(Globals.DEAD_STATUS);
				LOGGER.error(cnfexp.getLocalizedMessage());					
			}
		}

		return new Message("Error occurred while calling remote MDS",
				"",
				CompletionStatusCode.ERROR.name());
	}

	@Override
	public Message ls(final Directory root, final String filePath, final String... arguments)
			throws InvalidPropertiesFormatException {
		try
		{
			final StringBuffer resultCode = new StringBuffer();
			//Extract the relative path for the current partition.
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
							            (child.getSize() == null)?"0":child.getSize().toString(),
							            (child.getModifiedTimeStamp() == null)?
							            		"0":child.getModifiedTimeStamp().toString());
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
						final Message message = remoteExecCommand(remoteCommand.name(), 
								filePath, mdsServer, "");
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
					return new Message(filePath + " is in an unstable state",
							"",
							CompletionStatusCode.UNSTABLE.name());
				} 
				//If the path not found.
				else 
				{
					return new Message(filePath + " Does not exist",
							"",
							CompletionStatusCode.NOT_FOUND.name());
				}
			} 
			//If the path not found.
			else 
			{
				return new Message(filePath + " Does not exist",
						"",
						CompletionStatusCode.NOT_FOUND.name());
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
					Message replicaMessage = remoteExecCommand(command.name(), 
							fullPath, metaData, Globals.PRIMARY_MDS+":"+inodeNumber);
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
		
		if (directory != null) 
		{
			final Inode inode = directory.getInode();
			String resultcodeValue = resultCode.toString().trim();
			if (inode.getInodeNumber() != null && 
					Globals.PATH_FOUND.equals(resultcodeValue)) 
			{
				try
				{
					MetaDataServerInfo serverInfo = getRequiredMdsInfo(inode, true);
					if((primaryMessage) || (serverInfo != null && 
							Master.getMdsServerIpAddress().equals(serverInfo.getIpAddress())))
					{
						if (!directory.isFile()) 
						{
							final Directory node = new Directory(name, isFile, null);
							boolean fileExist = directory.getChildren().contains(node);
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
							final Long newInodeNumber;
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
								final Inode newInode = new Inode();
								newInode.setInodeNumber(newInodeNumber);
								newInode.getDataServerInfo().addAll(inode.getDataServerInfo());
								node.setInode(newInode);
								node.setModifiedTimeStamp(new Date().getTime());
								node.setSize(0L);
								directory.getChildren().add(node);
								if(primaryMessage)
								{
									return new Message(node.getName()+" created succesfully",
											directory.getInode().getDataServerInfo().toString(),
											CompletionStatusCode.SUCCESS.name());
								}
								final Message replicaMessages = updateReplicas(CommandsSupported.MKDIR,
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
						return remoteExecCommand(CommandsSupported.MKDIR.name(), 
												fullPath, 
												serverInfo,
												"");
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
				if (inode.getDataServerInfo() != null && 
						inode.getDataServerInfo().size() > 0) 
				{
					final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 
					return remoteExecCommand(CommandsSupported.MKDIR.name(), 
							fullPath, mdsServer, "");
				}
			} 
			else if (inode.getInodeNumber() != null) 
			{
				// need to add message explaining the unstable state of metadata.
				return new Message("MetaData in unstable state",
						"",
						CompletionStatusCode.UNSTABLE.name()); 			
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
		//Extract the relative path for the current partition.
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
		// Get the parent directory and the name of directory
		final String[] paths = searchablePath.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = searchablePath.substring(0, 
				searchablePath.length() - name.length() - 1);
		
		//Check if this is primary to replica update command.
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

		// Create the directory
		return create(root, dirPath, name, false, path,primaryMessage,inodeNumber);
	}

	@Override
	public Message touch(final Directory root, 
			final String path,
			String... arguments) 
			throws InvalidPropertiesFormatException 
	{
		// Extract the relative path for the current partition.
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
		// Get the parent directory and the name of directory
		final String[] paths = searchablePath.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = searchablePath.substring(0, searchablePath.length() - name.length() - 1);
		
		//Check this is a primary to replica update command.
		boolean primaryMessage = false;
		Long inodeNumber = null;
		if(arguments != null && arguments.length >= 2)
		{
			String[] primaryMessagesContent = arguments[1].split(":");
			primaryMessage = Globals.PRIMARY_MDS.equals(primaryMessagesContent[0].trim());
			if(primaryMessagesContent.length > 1 && 
					(primaryMessagesContent[1] != null && 
					!"null".equals(primaryMessagesContent[1].trim())))
			{
				inodeNumber = Long.valueOf(primaryMessagesContent[1].trim());
			}
		}

		StringBuffer resultCode = new StringBuffer();
		final Directory directory;
		//Get the partition root from the partition root list
		if(Globals.subTreePartitionList.containsKey(path))
		{
			directory = Globals.subTreePartitionList.get(path);
		}
		else // If not partition root then search the children of partition.
		{
			if(dirPath != null && 
					!"/".equals(dirPath.trim()) && 
					!"".equals(dirPath.trim()))
			{
				directory = search(root, dirPath, resultCode);
			}
			else
			{
				directory = root;
				resultCode.append(Globals.PATH_FOUND);
			}
		}
		//If search return a valid result.
		if(directory != null)
		{
			Inode inode = directory.getInode();
			String resultcodeValue = resultCode.toString().trim();
			
			//if the path leads to another MDS, then forward the command to the respective MDS 
			if(inode.getInodeNumber() == null && 
					(Globals.PARTIAL_PATH_FOUND.equals(resultcodeValue) || 
							Globals.PATH_FOUND.equals(resultcodeValue)))
			{
				if (inode.getDataServerInfo() != null && 
						inode.getDataServerInfo().size() > 0) 
				{
					final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 
					return remoteExecCommand(CommandsSupported.TOUCH.name(), 
							path, mdsServer, "");
				}
			}
			//If the parent Directory is found in the current MDS
			else if(inode.getInodeNumber() != null && 
					Globals.PATH_FOUND.equals(resultcodeValue))
			{
				// Create the file
				final Directory file = new Directory(name, true, null);
				final List<Directory> contents = directory.getChildren();
				boolean found = false;
				Directory touchFile = null;
				//If the end directory is the found directory
				if(directory.getName().equals(file.getName().trim()))
				{
					touchFile = directory;
					found = true;
				}
				else //Find the end directory in the child of found directory.
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
				//If the end file or Directory already exists.
				if(found && touchFile != null)
				{
					MetaDataServerInfo metaData = getRequiredMdsInfo(touchFile.getInode(), true);
					//If the end directory is present in another MDS, then forward the command
					//respective MDS
					if(touchFile.getInode().getInodeNumber() == null)
					{
						return remoteExecCommand(CommandsSupported.TOUCH.name(), 
								path, metaData, "");
					}					
					try
					{
						//If the parent directory is found in the current MDS 
						//and the current MDS is the primary for the parent directory
						//OR a primary message to update replica 
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
						//If not primary for the parent directory then forward to the primary MDS
						else
						{							
							return remoteExecCommand(CommandsSupported.TOUCH.name(), 
									path, metaData, "");
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
				else //If the end file is not found. Create a file node.
				{
					MetaDataServerInfo metaData = getRequiredMdsInfo(inode, true);
					try
					{
						//If the parent directory is found in the current MDS 
						//and the current MDS is the primary for the parent directory
						//OR a primary message to update replica 
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
								file.setModifiedTimeStamp(new Date().getTime());
								file.setSize(0L);
								directory.getChildren().add(file);
								//If primary to replica update then return after creating file in current MDS.
								if(primaryMessage)
								{
									return new Message(file.getName()+" created succesfully",
											directory.getInode().getDataServerInfo().toString(),
											CompletionStatusCode.SUCCESS.name());
								}
								//If current MDS is the primary then forward the commands to replicas
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
						else //If not primary for the parent directory then forward to the primary MDS
						{							
							return remoteExecCommand(CommandsSupported.TOUCH.name(), 
									path, metaData, "");
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
			//If the node exists in the current MDS but does match with the expected path.
			//Indication of unstable state.
			else if (inode.getInodeNumber() != null) 
			{
				return new Message("MetaData in unstable state",
						"",
						CompletionStatusCode.UNSTABLE.name()); // need to add message explaining the unstable state of
						// metadata.
			} 
			else //Path is not found. 
			{
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
	
	/**
	 * Removes a directory along with all its children.
	 * @param parentDir
	 * @param removeDir
	 * @param fullPath
	 * @return final status
	 */
	private Message removeNodeRecursively(final Directory parentDir,
			final Directory removeDir,
			final String fullPath,
			final String parentList,
			final boolean primaryMessage)
	{
		try
		{
			final List<Directory> remoteChildDirs = new ArrayList<>(); 
			final List<Directory> localEmptyChildDirs = new ArrayList<>();
			final List<Directory> localNonEmptyChildDirs = new ArrayList<>();
			boolean finalStatus = true;
			if(removeDir != null)
			{
				for(final Directory child:removeDir.getChildren())
				{
					final MetaDataServerInfo metadata = getRequiredMdsInfo(child.getInode(), true);
					if(child.getInode().getInodeNumber() != null &&
							((primaryMessage) || (metadata != null && 
							Master.getMdsServerIpAddress().equals(metadata.getIpAddress()))) &&
							(child.isFile() || child.isEmptyDirectory()))
					{
						localEmptyChildDirs.add(child);
					}
					else if(child.getInode().getInodeNumber() == null)
					{
						remoteChildDirs.add(child);
					}
					else if(!child.isEmptyDirectory())
					{
						localNonEmptyChildDirs.add(child);						
					}
				}
				removeDir.getChildren().removeAll(localEmptyChildDirs);
				for(final Directory child:localNonEmptyChildDirs)
				{					
					final String fullPathchild 
						= (fullPath.lastIndexOf('/') == fullPath.length()-1)?
							fullPath.substring(0, fullPath.lastIndexOf('/')):fullPath; 
					final Message localStatus = removeNodeRecursively(removeDir, 
									  child, 
									  fullPathchild+"/"+child.getName(), 
									  parentList, 
									  primaryMessage);
					finalStatus &= (localStatus != null && 
						CompletionStatusCode.SUCCESS.name()
							.equals(localStatus.getCompletionCode().toString().trim()));
				}
				for(final String path:Globals.subTreePartitionList.keySet())
				{				
					if(path.startsWith(fullPath) &&
							!path.substring(0, path.length()-1).equals(fullPath) &&
							!path.equals(fullPath))
					{
						final Message localStatus = removeNodeRecursively(null, 
								Globals.subTreePartitionList.get(path), 
								path,parentList,primaryMessage);
						finalStatus &= (localStatus != null && 
								CompletionStatusCode.SUCCESS.name()
									.equals(localStatus.getCompletionCode().toString().trim()));
					}
				}
				if(!primaryMessage)
				{
					for(final Directory remoteDir:remoteChildDirs)
					{
						final MetaDataServerInfo dataServerInfo = 
								getRequiredMdsInfo(remoteDir.getInode(),true);
						if( parentList == null || 
								"".equals(parentList) ||
								!parentList.contains(dataServerInfo.getServerName()))
						{
							final Message remoteStatus = remoteExecCommand(CommandsSupported.RMDIRF.name(), 
									fullPath+"/"+remoteDir.getName(), 
									dataServerInfo, 
									Globals.PARENT_MDS+":"+parentList+Master.getMdsServerId()+",");
							finalStatus &= (remoteStatus != null &&
									CompletionStatusCode.SUCCESS.name()
										.equals(remoteStatus.getCompletionCode().toString().trim()));
							if(remoteStatus != null && CompletionStatusCode.SUCCESS.name()
											.equals(remoteStatus.getCompletionCode().toString().trim()))
							{
								removeDir.getChildren().remove(remoteDir);
							}
						}
						else
						{
							removeDir.getChildren().remove(remoteDir);
						}
					}
				}
				else
				{
					removeDir.getChildren().removeAll(remoteChildDirs);
				}
				if(removeDir.isEmptyDirectory())
				{
					if(parentDir != null)
					{
						parentDir.getChildren().remove(removeDir);
					}
					else
					{
						Globals.subTreePartitionList.remove(fullPath);
					}
					if(finalStatus)
					{
						return new Message(fullPath+" removed successfully",
							"",
							CompletionStatusCode.SUCCESS.name());
					}
					else
					{
						return new Message("error occurred while removing children of "+fullPath,
								"",
								CompletionStatusCode.ERROR.name());
					}
				}
				else
				{
					return new Message("error occurred while removing children of "+fullPath,
							"",
							CompletionStatusCode.ERROR.name());
				}
			}
		}
		catch(Exception exp)
		{
			return new Message(exp.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name());
		}
		return new Message("No Error or No processing done",
				"",
				CompletionStatusCode.ERROR.name());
	}
	
	/**
	 * This function is used to remove a directory from the tree.
	 * @param root
	 * @param currentPath
	 * @param name
	 * @param isFile
	 * @param fullPath
	 * @param primaryMessage
	 * @param forceRemove
	 * @return remove directory status message
	 */
	private Message removeNode(final Directory root, 
			final String currentPath,
			final String name, 
			final boolean isFile,
			final String fullPath,
			final String messageHeader,
			final boolean forceRemove)
	{
		boolean primaryMessage = false;
		boolean parentMessage = false;
		final String parentList;
		if(messageHeader != null)
		{			
			String[] primaryMessagesContent = messageHeader.split(":");
			primaryMessage = Globals.PRIMARY_MDS.equals(primaryMessagesContent[0].trim());
			parentMessage = Globals.PARENT_MDS.equals(primaryMessagesContent[0].trim());
			if(parentMessage)
			{
				parentList = primaryMessagesContent[1];
			}
			else
			{
				parentList = "";
			}
		}
		else
		{
			parentList = "";
		}
		
		// Search and get to the directory where we have to create
		final StringBuffer resultCode = new StringBuffer();
		final Directory directory;
		
		if(Globals.subTreePartitionList.containsKey(fullPath))
		{
			directory = Globals.subTreePartitionList.get(fullPath);
			if(directory.isEmptyDirectory())
			{
				final Inode inode = directory.getInode();
				final MetaDataServerInfo metadata = getRequiredMdsInfo(inode, true);
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
						return remoteExecCommand(CommandsSupported.RMDIR.name(), 
								fullPath, metadata, Globals.PRIMARY_MDS+":");
					}
				}
				catch(Exception unexp)
				{
					return new Message(unexp.getLocalizedMessage(),
							"",
							CompletionStatusCode.ERROR.name());
				}
			}
			else if(forceRemove)
			{
				final Message removeRecur = removeNodeRecursively(null,
						directory,
						fullPath,
						parentList,
						primaryMessage);
				if(removeRecur != null && 
						CompletionStatusCode.SUCCESS.name()
							.equals(removeRecur.getCompletionCode().toString().trim()))
				{
					if(!primaryMessage)
					{
						Message finalMessage = updateReplicas(CommandsSupported.RMDIRF, 
								directory.getInode(), fullPath, null);
						if(CompletionStatusCode.SUCCESS.name()
								.equals(finalMessage.getCompletionCode()
										.toString().trim()))
						{
							return new Message(name+" removed successfully",
									"",
									CompletionStatusCode.SUCCESS.name());
						}
						return new Message(finalMessage.getContent(),
								directory.getInode().getDataServerInfo().toString(),
								CompletionStatusCode.ERROR.name());
					}					
				}
				return removeRecur;
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
				
				String resultcodeValue = resultCode.toString().trim();
				
				if(inode.getInodeNumber() == null && 
						(Globals.PARTIAL_PATH_FOUND.equals(resultcodeValue) || 
								(Globals.PATH_FOUND.equals(resultcodeValue))))
				{
					if (inode.getDataServerInfo() != null && 
							inode.getDataServerInfo().size() > 0) 
					{
						final MetaDataServerInfo mdsServer = getRequiredMdsInfo(inode, true); 
						return remoteExecCommand(CommandsSupported.RMDIR.name(), 
								fullPath, mdsServer, "");
					}
				}
				else if(inode.getInodeNumber() != null && 
						Globals.PATH_FOUND.equals(resultcodeValue))
				{
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
							MetaDataServerInfo metaData 
								= getRequiredMdsInfo(removeDirectory.getInode(), true);
							if(removeDirectory.getInode().getInodeNumber() == null)
							{
								final Message remoteMessage 
									= remoteExecCommand((forceRemove)?CommandsSupported.RMDIRF.name():
											CommandsSupported.RMDIR.name(), 
										fullPath, metaData, "");
								if((remoteMessage != null) &&
										CompletionStatusCode.SUCCESS.name()
											.equals(remoteMessage
														.getCompletionCode().toString().trim()))
								{
									directory.getChildren().remove(removeDirectory);
								}
								return remoteMessage;
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
									return remoteExecCommand(CommandsSupported.RMDIR.name(), 
											fullPath, metaData, "");
								}
							}
							catch(Exception unexp)
							{
								return new Message(unexp.getLocalizedMessage(),
										"",
										CompletionStatusCode.ERROR.name());
							}
						}
						else if(forceRemove)
						{
							final Message removeRecur = removeNodeRecursively(directory,
									removeDirectory,
									fullPath,
									parentList,
									primaryMessage);
							if(removeRecur != null && 
									CompletionStatusCode.SUCCESS.name()
										.equals(removeRecur.getCompletionCode().toString().trim()))
							{
								if(!primaryMessage)
								{
									Message finalMessage = updateReplicas(CommandsSupported.RMDIRF, 
											directory.getInode(), fullPath, null);
									if(CompletionStatusCode.SUCCESS.name()
											.equals(finalMessage.getCompletionCode()
													.toString().trim()))
									{
										return new Message(name+" removed successfully",
												"",
												CompletionStatusCode.SUCCESS.name());
									}
									return new Message(finalMessage.getContent(),
											directory.getInode().getDataServerInfo().toString(),
											CompletionStatusCode.ERROR.name());
								}								
							}
							return removeRecur;
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
					return new Message("MetaData in unstable state",
							"",
							CompletionStatusCode.UNSTABLE.name()); // need to add message explaining the unstable state of
							// metadata.
				} 
				else 
				{
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
		
		return new Message("No Error or No Action. Something went worng",
				"",
				CompletionStatusCode.ERROR.name());
	}

	@Override
	public Message rmdir(final Directory root, 
			final String path, 
			final String... arguments)
			throws InvalidPropertiesFormatException {
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
		if("".equals(searchablePath))
		{
			searchablePath = path;
		}
		final String[] paths = searchablePath.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = searchablePath.substring(0, searchablePath.length() - name.length() - 1);		

		// True for RMDIRF i.e. rmdir -f option
		final boolean isForceRemove = arguments != null 
		        && arguments[arguments.length - 1].equals("-f");
					
		return removeNode(root, dirPath, name, 
				false, path, 
				(arguments != null && arguments.length > 1)?arguments[1]:"", 
				isForceRemove);
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
						final Message message = remoteExecCommand(CommandsSupported.CD.name(), 
								filePath, mdsServer, "");
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
					return new Message(filePath + " is in an unstable state");
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
     
    /**
	 * Performs a tree search from the {@literal root} on the directory
	 * structure corresponding to the {@literal filePath}
	 *
	 * @param root
	 *            Root of directory structure
	 * @param filePath
	 *            Path to search
	 * @return list of matching Nodes in the path, empty list if not found
	 */
	private List<Directory> fetchDirsInPath(final Directory root, 
			final String filePath, 
			final StringBuffer resultCode) {		
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
		final List<Directory> dirsInPath = new ArrayList<>();
		Directory curNode= root;
		// Find the directory in directory tree
		for (final String path : paths) {
			// Match the root
			boolean found = false;
			if (curNode.getName()
					.equals(path)) {
				found = true;
				countLevel++;
				dirsInPath.add(curNode);
				Long operationCounter = curNode.getOperationCounter();
				operationCounter++;
				curNode.setOperationCounter(operationCounter);
			}
			
			if(curNode.getChildren() != null)
			{
				// Check if the path corresponds to any child in this directory
				for (final Directory child : curNode.getChildren()) {	
					if (child.getName()
							.equals(path)) {
						curNode = child;
						found = true;		
						countLevel++;
						dirsInPath.add(curNode);
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
				return dirsInPath;
			}
		}

		// Return the node where the path was found
		resultCode.append(Globals.PATH_FOUND);
		return dirsInPath;
	}
    
	@Override
	public Message acquireReadLocks(final Directory root, 
			final String filePath, 
			final String... arguments) 
	{
		try
		{
			String searchablePath;
			if (arguments != null && 
					arguments.length > 0 && 
					(!"/".equals(arguments[0]) && !"root".equals(arguments[0]))) {
				searchablePath = filePath.substring(arguments[0].length());
			} else {
				searchablePath = filePath;
			}
			final List<Directory> toLockDirs = new ArrayList<>();
			final StringBuffer resultCode = new StringBuffer();
			
			final String clientId = arguments[arguments.length -1];
			
			if(searchablePath != null && 
					!"".equals(searchablePath) &&
					!"root".equals(searchablePath) &&
					!"/".equals(searchablePath))
			{
				toLockDirs.addAll(fetchDirsInPath(
									root, 
									searchablePath, 
									resultCode));
			}
			else
			{
				toLockDirs.add(root);
				resultCode.append(Globals.PATH_FOUND);
			}
			
			if(!toLockDirs.isEmpty())
			{
				final Directory node = toLockDirs.get(toLockDirs.size()-1);
				final Inode lastNodeInode = node.getInode();
				final String resultCodeValue = resultCode.toString().trim();
				if(lastNodeInode.getInodeNumber() != null &&
						Globals.PATH_FOUND.equals(resultCodeValue))
				{				
					final boolean lockStatus 
						= CephDirectoryOperations.lockDirectories(toLockDirs, 
																	null, 
																	clientId);
					if(!lockStatus)
					{
						return new Message("Not able to read lock all the directories in "+ filePath,
								lastNodeInode.getDataServerInfo().toString(),
								CompletionStatusCode.ERROR.name());
					}
					
					return new Message(filePath + " read locked successfully",
							lastNodeInode.getDataServerInfo().toString(),
							CompletionStatusCode.SUCCESS.name());
				}
				else if(lastNodeInode.getInodeNumber() == null &&
						(Globals.PATH_FOUND.equals(resultCodeValue) ||
								Globals.PARTIAL_PATH_FOUND.equals(resultCodeValue)))
				{
					toLockDirs.remove(toLockDirs.size() -1);
					final boolean lockStatus 
						= CephDirectoryOperations.lockDirectories(toLockDirs, 
																	null, 
																	clientId);
					if(!lockStatus)
					{
						return new Message("Not able to read lock all the directories in "+ filePath,
								lastNodeInode.getDataServerInfo().toString(),
								CompletionStatusCode.ERROR.name());
					}
					else
					{
						final MetaDataServerInfo mdsServer = getRequiredMdsInfo(lastNodeInode, 
																true);
						final Message remoteStatus = remoteExecCommand(Globals.ACQUIRE_READ_LOCK, 
									filePath, 
									mdsServer, 
									clientId);
						
						if(remoteStatus != null &&
								(CompletionStatusCode.SUCCESS.name()
										.equals(remoteStatus.getCompletionCode().toString().trim()) ||
										CompletionStatusCode.NOT_FOUND.name()
											.equals(remoteStatus.getCompletionCode().toString().trim())))
						{
							return remoteStatus;
						}
						if(toLockDirs != null && 
								!toLockDirs.isEmpty())
						{
							CephDirectoryOperations.unlockDirectories(toLockDirs, 
																		null, 
																		clientId);
						}
						return new Message("Not able to read lock all the nodes in "+ filePath,
								remoteStatus.getHeader(),
								CompletionStatusCode.ERROR.name());
					}
				}
				//If partial path found but the inode indicates 
				//that the path existing in current MDS
				else if (lastNodeInode.getInodeNumber() != null && 
						!Globals.PARTIAL_PATH_FOUND.equals(resultCodeValue)) 
				{
					return new Message(filePath + " is in an unstable state",
							"",
							CompletionStatusCode.UNSTABLE.name());
				} 
				//If the path not found.
				else 
				{
					return new Message(filePath + " Does not exist",
							"",
							CompletionStatusCode.NOT_FOUND.name());
				}
			}
		}
		catch(Exception ex)
		{
			return new Message(ex.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name());
		}
		return new Message("Read lock command completed without any errors and proper output",
				"",
				CompletionStatusCode.ERROR.name());
	}

	@Override
	public Message acquireWriteLocks(final Directory root, 
			final String filePath, 
			final String... arguments) 
	{
		try
		{
			String searchablePath;
			if (arguments != null && 
					arguments.length > 0 && 
					(!"/".equals(arguments[0]) && 
							!"root".equals(arguments[0]))) {
				searchablePath = filePath.substring(arguments[0].length());
			} else {
				searchablePath = filePath;
			}
			
			final String clientId = arguments[arguments.length -1];
			
			final List<Directory> toLockDirs = new ArrayList<>();
			final StringBuffer resultCode = new StringBuffer();
			if(searchablePath != null && 
					!"".equals(searchablePath) &&
					!"root".equals(searchablePath) &&
					!"/".equals(searchablePath))
			{
				toLockDirs.addAll(fetchDirsInPath(
									root, 
									searchablePath, 
									resultCode));
			}
			else
			{
				toLockDirs.add(root);
				resultCode.append(Globals.PATH_FOUND);
			}
			if(!toLockDirs.isEmpty())
			{
				final Directory node = toLockDirs.get(toLockDirs.size()-1);
				final Inode lastNodeInode = node.getInode();
				final String resultCodeValue = resultCode.toString().trim();
				if(lastNodeInode.getInodeNumber() != null &&
						Globals.PATH_FOUND.equals(resultCodeValue))
				{				
					if(!toLockDirs.isEmpty())
					{
						final Directory writeLockDir = toLockDirs.remove(toLockDirs.size() -1);
						final boolean lockStatus 
							= CephDirectoryOperations.lockDirectories(toLockDirs, 
																		writeLockDir, 
																		clientId);
						if(!lockStatus)
						{
							return new Message("Not able to complete write lock the "+ filePath,
									lastNodeInode.getDataServerInfo().toString(),
									CompletionStatusCode.ERROR.name());
						}
					}					
					
					return new Message(filePath + " write locked successfully",
							lastNodeInode.getDataServerInfo().toString(),
							CompletionStatusCode.SUCCESS.name());
				}
				else if(lastNodeInode.getInodeNumber() == null &&
						(Globals.PATH_FOUND.equals(resultCodeValue) ||
								Globals.PARTIAL_PATH_FOUND.equals(resultCodeValue)))
				{
					toLockDirs.remove(toLockDirs.size() -1);
					final boolean lockStatus 
						= CephDirectoryOperations.lockDirectories(toLockDirs, 
																	null, 
																	clientId);
					if(!lockStatus)
					{						
						return new Message("Not able to write lock all the directories in "+ filePath,
								lastNodeInode.getDataServerInfo().toString(),
								CompletionStatusCode.ERROR.name());
					}
					else
					{
						final MetaDataServerInfo mdsServer = getRequiredMdsInfo(lastNodeInode, 
																true);
						final Message remoteStatus = remoteExecCommand(Globals.ACQUIRE_WRITE_LOCK, 
									filePath, 
									mdsServer, 
									clientId);
						if(remoteStatus != null &&
								(CompletionStatusCode.SUCCESS.name()
									.equals(remoteStatus.getCompletionCode().toString().trim()) ||
									CompletionStatusCode.NOT_FOUND.name()
										.equals(remoteStatus.getCompletionCode().toString().trim())))
						{
							return remoteStatus;
						}
						if(toLockDirs != null && 
								!toLockDirs.isEmpty())
						{
							CephDirectoryOperations.unlockDirectories(toLockDirs, 
																		null, 
																		clientId);
						}
						return new Message("Not able to write lock the directory in "+ filePath,
								remoteStatus.getHeader(),
								CompletionStatusCode.ERROR.name());
					}
				}
				//If partial path found but the inode indicates 
				//that the path existing in current MDS
				else if (lastNodeInode.getInodeNumber() != null && 
						!Globals.PARTIAL_PATH_FOUND.equals(resultCodeValue)) 
				{
					return new Message(filePath + " is in an unstable state",
							"",
							CompletionStatusCode.UNSTABLE.name());
				} 
				//If the path not found.
				else 
				{
					return new Message(filePath + " Does not exist",
							"",
							CompletionStatusCode.NOT_FOUND.name());
				}
			}
		}
		catch(Exception ex)
		{
			return new Message(ex.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name());
		}
		return new Message("Write lock command completed without any errors and proper output",
				"",
				CompletionStatusCode.ERROR.name());
	}

	@Override
	public Message releaseReadLocks(final Directory root, 
			final String filePath, 
			final String... arguments) 
	{
		try
		{
			String searchablePath;
			if (arguments != null && 
					arguments.length > 0 && 
					(!"/".equals(arguments[0]) && !"root".equals(arguments[0]))) {
				searchablePath = filePath.substring(arguments[0].length());
			} else {
				searchablePath = filePath;
			}
			
			final String clientId = arguments[arguments.length -1];
			
			final List<Directory> toUnLockDirs = new ArrayList<>();
			final StringBuffer resultCode = new StringBuffer();
			if(searchablePath != null && 
					!"".equals(searchablePath) &&
					!"root".equals(searchablePath) &&
					!"/".equals(searchablePath))
			{
				toUnLockDirs.addAll(fetchDirsInPath(
									root, 
									searchablePath, 
									resultCode));
			}
			else
			{
				toUnLockDirs.add(root);
				resultCode.append(Globals.PATH_FOUND);
			}
			if(!toUnLockDirs.isEmpty())
			{
				final Directory node = toUnLockDirs.get(toUnLockDirs.size()-1);
				final Inode lastNodeInode = node.getInode();
				final String resultCodeValue = resultCode.toString().trim();
				if(lastNodeInode.getInodeNumber() != null &&
						Globals.PATH_FOUND.equals(resultCodeValue))
				{
					CephDirectoryOperations.unlockDirectories(toUnLockDirs, 
																null, 
																clientId);
					
					return new Message(filePath + " read unlocked successfully",
							lastNodeInode.getDataServerInfo().toString(),
							CompletionStatusCode.SUCCESS.name());
				}
				else if(lastNodeInode.getInodeNumber() == null &&
						(Globals.PATH_FOUND.equals(resultCodeValue) ||
								Globals.PARTIAL_PATH_FOUND.equals(resultCodeValue)))
				{
					toUnLockDirs.remove(toUnLockDirs.size() -1);
					CephDirectoryOperations.unlockDirectories(toUnLockDirs, 
																null, 
																clientId);
					final MetaDataServerInfo mdsServer = getRequiredMdsInfo(lastNodeInode, 
															true);
					final Message remoteStatus = remoteExecCommand(Globals.RELEASE_READ_LOCK, 
														filePath, 
														mdsServer, 
														clientId);
					if(remoteStatus != null &&
							CompletionStatusCode.SUCCESS.name()
								.equals(remoteStatus.getCompletionCode().toString().trim()))
					{
						return remoteStatus;
					}

					return new Message("Not able to unlock all read locks of all the nodes in "+ filePath,
							remoteStatus.getHeader(),
							CompletionStatusCode.ERROR.name());					
				}
				//If partial path found but the inode indicates 
				//that the path existing in current MDS
				else if (lastNodeInode.getInodeNumber() != null && 
						!Globals.PARTIAL_PATH_FOUND.equals(resultCodeValue)) 
				{
					return new Message(filePath + " is in an unstable state",
							"",
							CompletionStatusCode.UNSTABLE.name());
				} 
				//If the path not found.
				else 
				{
					return new Message(filePath + " Does not exist",
							"",
							CompletionStatusCode.NOT_FOUND.name());
				}
			}
		}
		catch(Exception ex)
		{
			return new Message(ex.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name());
		}
		return new Message("Read unlock command completed without any errors and proper output",
				"",
				CompletionStatusCode.ERROR.name());
	}

	@Override
	public Message releaseWriteLocks(final Directory root, 
			final String filePath, 
			final String... arguments) 
	{
		try
		{
			String searchablePath;
			if (arguments != null && 
					arguments.length > 0 && 
					(!"/".equals(arguments[0]) && !"root".equals(arguments[0]))) {
				searchablePath = filePath.substring(arguments[0].length());
			} else {
				searchablePath = filePath;
			}
			
			final String clientId = arguments[arguments.length -1];
			
			final List<Directory> toUnLockDirs = new ArrayList<>();
			final StringBuffer resultCode = new StringBuffer();
			if(searchablePath != null && 
					!"".equals(searchablePath) &&
					!"root".equals(searchablePath) &&
					!"/".equals(searchablePath))
			{
				toUnLockDirs.addAll(fetchDirsInPath(
									root, 
									searchablePath, 
									resultCode));
			}
			else
			{
				toUnLockDirs.add(root);
				resultCode.append(Globals.PATH_FOUND);
			}
			if(!toUnLockDirs.isEmpty())
			{
				final Directory node = toUnLockDirs.get(toUnLockDirs.size()-1);
				final Inode lastNodeInode = node.getInode();
				final String resultCodeValue = resultCode.toString().trim();
				if(lastNodeInode.getInodeNumber() != null &&
						Globals.PATH_FOUND.equals(resultCodeValue))
				{
					final Directory writeUnlockDir = toUnLockDirs.remove(toUnLockDirs.size() -1);
					CephDirectoryOperations.unlockDirectories(toUnLockDirs, 
																writeUnlockDir, 
																clientId);
					
					return new Message(filePath + " write unlocked successfully",
							lastNodeInode.getDataServerInfo().toString(),
							CompletionStatusCode.SUCCESS.name());
				}
				else if(lastNodeInode.getInodeNumber() == null &&
						(Globals.PATH_FOUND.equals(resultCodeValue) ||
								Globals.PARTIAL_PATH_FOUND.equals(resultCodeValue)))
				{										
					final MetaDataServerInfo mdsServer = getRequiredMdsInfo(lastNodeInode, 
															true);
					final Message remoteStatus = remoteExecCommand(Globals.RELEASE_WRITE_LOCK, 
								filePath, 
								mdsServer, 
								clientId);
					if(remoteStatus != null &&
							CompletionStatusCode.SUCCESS.name()
								.equals(remoteStatus.getCompletionCode().toString().trim()))
					{
						toUnLockDirs.remove(toUnLockDirs.size() -1);
						CephDirectoryOperations.unlockDirectories(toUnLockDirs, 
																	null, 
																	clientId);
						return remoteStatus;
					}					
					return new Message("Not able to unlock the write lock in the directory "+ filePath,
							remoteStatus.getHeader(),
							CompletionStatusCode.ERROR.name());
				}
				//If partial path found but the inode indicates 
				//that the path existing in current MDS
				else if (lastNodeInode.getInodeNumber() != null && 
						!Globals.PARTIAL_PATH_FOUND.equals(resultCodeValue)) 
				{
					return new Message(filePath + " is in an unstable state",
							"",
							CompletionStatusCode.UNSTABLE.name());
				} 
				//If the path not found.
				else 
				{
					return new Message(filePath + " Does not exist",
							"",
							CompletionStatusCode.NOT_FOUND.name());
				}
			}
		}
		catch(Exception ex)
		{
			return new Message(ex.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name());
		}
		return new Message("Write unlock command completed without any errors and proper output",
				"",
				CompletionStatusCode.ERROR.name());
	}
	
	private static synchronized boolean lockDirectories(final List<Directory> readLockDirs,
			final Directory writeLockDir,
			final String clientId)
	{
		boolean finalStatus = true;
		if(readLockDirs != null && !readLockDirs.isEmpty())
		{
			for(final Directory dir:readLockDirs)
			{				
				if((dir.getWriteLockClient() == null) || 
						"".equals(dir.getWriteLockClient()))
				{
					dir.getReadLockClients().add(clientId);
					finalStatus &= true;
				}
				else
				{
					finalStatus = false;
					break;
				}
			}
		}
		if(!finalStatus)
		{
			if(readLockDirs != null)
			{
				for(final Directory dir:readLockDirs)
				{				
					dir.getReadLockClients().remove(clientId);
				}
			}
		}
		if(finalStatus && writeLockDir != null)
		{
			if((writeLockDir.getWriteLockClient() == null || 
					"".equals(writeLockDir.getWriteLockClient())) &&
					((writeLockDir.getReadLockClients() == null) || 
						(writeLockDir.getReadLockClients() != null &&
								writeLockDir.getReadLockClients().isEmpty()))
					)
			{
				writeLockDir.setWriteLockClient(clientId);
				finalStatus &= true;
			}
			else
			{
				if(readLockDirs != null)
				{
					for(final Directory dir:readLockDirs)
					{				
						dir.getReadLockClients().remove(clientId);
					}
				}
				finalStatus = false;
			}
		}
		return finalStatus;
	}
    
	private static synchronized boolean unlockDirectories(final List<Directory> readLockDirs,
			final Directory writeUnLockDir,
			final String clientId)
	{		
		boolean finalStatus = true;
		if(readLockDirs != null && !readLockDirs.isEmpty())
		{
			for(final Directory dir:readLockDirs)
			{				
				dir.getReadLockClients().remove(clientId);
				finalStatus &= true;
			}
		}
		if(finalStatus && writeUnLockDir != null)
		{
			if(writeUnLockDir.getWriteLockClient() != null &&
					writeUnLockDir.getWriteLockClient().equals(clientId))
			{
				writeUnLockDir.setWriteLockClient(null);
			}			
		}
		return finalStatus;
	}
}