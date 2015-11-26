package client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import commons.AppConfig;
import commons.CommandsSupported;
import commons.CompletionStatusCode;
import commons.Globals;
import commons.Message;
import master.metadata.MetaDataServerInfo;

/**
 * This client takes care of ceph command execution and caching.
 * @author jaykay
 *
 */
public class CephClient {
	
	private final Socket				socket;
	private final ObjectInputStream		inputStream;
	private final ObjectOutputStream	outputStream;
	private final HashMap<String,List<MetaDataServerInfo>> cachedServers = new HashMap<>();
	private final static Logger			LOGGER	= Logger.getLogger(CephClient.class);
	private static final String         ROOT    = "root";
	private static String               pwd     = ROOT;
	
	/**
	 * Constructor
	 * @throws InvalidPropertiesFormatException 
	 *
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public CephClient() throws IOException
	{
		// Initialize configuration
		new AppConfig("conf");
		LOGGER.setLevel(Level.INFO);
		
		final MetaDataServerInfo rootMds = new MetaDataServerInfo();
		rootMds.setIpAddress(AppConfig.getValue("client.ceph.root"));
		rootMds.setStatus("Alive");
		rootMds.setServerName("");
		
		final List<MetaDataServerInfo> rootMdsServers = new ArrayList<>();
		rootMdsServers.add(rootMds);
		cachedServers.put("/", rootMdsServers);
		
		socket = new Socket(AppConfig.getValue("client.masterIp"), Integer.parseInt(AppConfig.getValue("client.masterPort")));
		outputStream = new ObjectOutputStream(socket.getOutputStream());
		inputStream = new ObjectInputStream(socket.getInputStream());
	}
	
	/**
	 * Get the MDS server info to forward the command (read/write) to the
	 * respective MDS.
	 * 
	 * @param inode
	 * @param isWrite
	 * @return MDS information
	 */
	private Socket getRequiredMdsSocket(
			final List<MetaDataServerInfo> mdsServers, 
			final boolean isWrite) {
		MetaDataServerInfo serverInfo = null;
		Socket mdsServerSocket = null;
		for (final MetaDataServerInfo info : mdsServers) 
		{
			if (Globals.ALIVE_STATUS.equalsIgnoreCase(info.getStatus()) && 
					(isWrite && Globals.PRIMARY_MDS.equals(info.getServerType()) || 
							!isWrite)) {
				serverInfo = info;
				break;
			}
		}
		if(serverInfo != null)
		{
			try
			{
				if(serverInfo.getIpAddress().equals(AppConfig.getValue("client.masterIp")))
				{
					mdsServerSocket = this.socket;
				}
				else
				{
					mdsServerSocket = new Socket(serverInfo.getIpAddress(), 
							Integer.parseInt(AppConfig.getValue("client.masterPort")));
				}
			}
			catch (NumberFormatException e) {
				LOGGER.error("Error occured while executing commands", e);
				System.exit(0);
			} catch (IOException e) {
				LOGGER.error("Error occured while executing commands", e);
				System.exit(0);
			}
		}
		return mdsServerSocket;
	}
	
	private boolean acquireLocks(final String command, 
			final StringBuffer lockedPath,
			final boolean acquireParentLock,
			final boolean isReadLock)
	{
		boolean status = false;
		try
		{			
			final String[] commandParse = command.split(" ");
			final String executableCommand = commandParse[1].trim();
			final String name;
			final String dirPath;
			if(!"".equals(executableCommand) && 
					!"/".equals(executableCommand) &&
					!"root".equals(executableCommand))
			{
				final String[] paths = executableCommand.split("/");
				name = paths[paths.length - 1];
				dirPath = executableCommand.substring(0,
							executableCommand.length() - name.length() - 1);
			}
			else
			{
				dirPath = executableCommand;
				name = "";
			}
			
			if(acquireParentLock)
			{
				lockedPath.append(dirPath);
			}
			else
			{
				lockedPath.append(executableCommand);
			}
			
			// Send command to master					
			outputStream.writeObject(new Message(
					(isReadLock?Globals.ACQUIRE_READ_LOCK:Globals.ACQUIRE_WRITE_LOCK)
					+" "+lockedPath.toString()));
			outputStream.flush();
			
			// Wait and read the reply
			final Message lockMessage = (Message) inputStream.readObject();
			if(CompletionStatusCode.SUCCESS.name()
					.equals(lockMessage.getCompletionCode().toString().trim()))
			{
				if(lockMessage.getHeader() != null && 
						!"".equals(lockMessage.getHeader()))
				{
					final List<MetaDataServerInfo> newMdsServers 
						= MetaDataServerInfo.fromStringToMetadata(
							lockMessage.getHeader().trim());
					if(!newMdsServers.isEmpty())
					{
						this.cachedServers.put(lockedPath.toString(), 
								newMdsServers);
					}
				}
				status = true;
			}
			else if(CompletionStatusCode.NOT_FOUND.name()
					.equals(lockMessage.getCompletionCode().toString().trim()))
			{
				LOGGER.error(lockMessage);
				this.cachedServers.remove(lockedPath.toString());
				status = false;
			}				
			else
			{
				LOGGER.error(lockMessage);
				status = false;
			}
		}
		catch(ClassNotFoundException cnfexp)
		{
			LOGGER.error(new Message(cnfexp.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name()));
		}
		catch(IOException ioexp)
		{
			LOGGER.error(new Message(ioexp.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name()));
		}		
		return status;
	}
	
	/**
	 * Execute commands listed in a file
	 *
	 * @param inputFileName
	 *            File that contains the commands
	 */
	public void executeCommands(final String inputFileName) {
		int number = 0;

		// Read commands
		try (Scanner scanner = new Scanner(System.in)) {
			while (scanner.hasNext()) {
				String command = scanner.nextLine();	
				boolean readLockAcquired = false;
				boolean writeLockAcquired = false;
				final String lockedPath;

				if("EXIT".equals(command))
				{
					break;
				}
				else if(command.startsWith(CommandsSupported.CD.name()))
				{
					final String argument = command.substring(3);
					if(!argument.startsWith(ROOT)) {
						command = new String(Paths.get(pwd, argument).toString());
					}
				}
				else if(command.startsWith(CommandsSupported.PWD.name())) 
				{
					LOGGER.info("Command " + number + " : " + command);
					LOGGER.info(pwd + "\n");
					number++;
					continue;
				}
				
				final String[] commandParse = command.split(" ");
				
				if(command.startsWith(CommandsSupported.LS.name()) || 
						command.startsWith(CommandsSupported.LSL.name()) ||
						command.startsWith(CommandsSupported.CD.name()))
				{								
					final StringBuffer lockedPathBuf = new StringBuffer();
					final boolean lockStatus = acquireLocks(command, 
							lockedPathBuf, 
							false, 
							true);
					if(lockStatus)
					{
						readLockAcquired = true;
						lockedPath = lockedPathBuf.toString();
					}
					else
					{
						continue;
					}
				}
				else if(command.startsWith(CommandsSupported.MKDIR.name()) ||
						command.startsWith(CommandsSupported.RMDIR.name()) ||
						command.startsWith(CommandsSupported.RMDIRF.name()))
				{
					final StringBuffer lockedPathBuf = new StringBuffer();
					final boolean lockStatus = acquireLocks(command, 
							lockedPathBuf, 
							command.startsWith(CommandsSupported.MKDIR.name()), 
							false);
					if(lockStatus)
					{
						writeLockAcquired = true;
						lockedPath = lockedPathBuf.toString();
					}
					else
					{
						continue;
					}									
				}
				else
				{
					final StringBuffer lockedPathBuf = new StringBuffer();
					final boolean lockFullPathStatus = acquireLocks(command, 
							lockedPathBuf, 
							false, 
							false);
					if(lockFullPathStatus)
					{
						writeLockAcquired = true;
						lockedPath = lockedPathBuf.toString();
					}
					else
					{
						final boolean lockParent = acquireLocks(command, 
								lockedPathBuf, 
								true, 
								false);
						if(lockParent)
						{
							writeLockAcquired = true;
							lockedPath = lockedPathBuf.toString();
						}
						else
						{
							continue;
						}
					}
				}
				final StringBuffer partialFilePath = new StringBuffer();
				
				final List<MetaDataServerInfo> mdsServers 
								= MetaDataServerInfo.findClosestServer(command
								.substring(commandParse[0].length()+1), 
								partialFilePath, this.cachedServers);
								
				final Socket mdsServerSocket = getRequiredMdsSocket(mdsServers, true);
				// Send command to master
				
				ObjectInputStream inputMdsStream 
							= new ObjectInputStream(mdsServerSocket.getInputStream());
				ObjectOutputStream outputMdsStream
							= new ObjectOutputStream(mdsServerSocket.getOutputStream());
				outputMdsStream.writeObject(new Message(command));
				outputMdsStream.flush();

				// Exit if command is exit
				if (command.equalsIgnoreCase(CommandsSupported.EXIT.name())) {
					LOGGER.info("Command " + number + " : " + command);
					break;
				}

				// Wait and read the reply
				final Message message = (Message) inputMdsStream.readObject();
				final String reply = message.getContent();
				final String header = message.getHeader();
				if(message.getCompletionCode() != null
						&& !"".equals(message.getCompletionCode().toString().trim())
						&& CompletionStatusCode.SUCCESS.name()
							.equals(message.getCompletionCode().toString().trim())
						&& header != null 
						&& !"".equals(header.trim()))
				{
					//Update the client cache of MDS cluster map.
					final List<MetaDataServerInfo> newMdsServers 
						= MetaDataServerInfo.fromStringToMetadata(header.trim());
					if(!newMdsServers.isEmpty())
					{
						this.cachedServers.put(command
								.substring(commandParse[0].length()+1)
								, newMdsServers);
					}
					//Update the pwd if the executed command is CD
					if(command.startsWith(CommandsSupported.CD.name())) {
						final String argument = command.substring(3);
						pwd = argument.startsWith(ROOT)
								? argument
										: Paths.get(pwd, argument).toString();						
					}
				}
				LOGGER.info("Command " + number + " : " + command);
				LOGGER.info(reply + "\n");

				number++;
				if(mdsServerSocket != this.socket)
				{
					mdsServerSocket.close();
				}				
				
				if(readLockAcquired && lockedPath != null)
				{
					outputStream.writeObject(new Message(Globals.RELEASE_READ_LOCK+" "+
										lockedPath));
					outputStream.flush();
				}
				else if(writeLockAcquired && lockedPath != null)
				{
					outputStream.writeObject(new Message(Globals.RELEASE_WRITE_LOCK+" "+
							lockedPath));
					outputStream.flush();
				}
				
				final Message unLockMessage = (Message) inputStream.readObject();
				LOGGER.debug(unLockMessage);
					
			}
		} catch (final IOException | ClassNotFoundException e) {
			LOGGER.error("Error occured while executing commands", e);
			System.exit(0);
		}
		finally
		{
			try
			{
				socket.close();
			}
			catch(IOException ioexp)
			{
				LOGGER.error(new Message(ioexp.getLocalizedMessage(),
						"",
						CompletionStatusCode.ERROR.name()));
			}
		}
	}

	public static void main(String[] args) {
		// Create ceph client
		CephClient client = null;
		try {
			client = new CephClient();
		} catch (final IOException e) {
			LOGGER.error("", e);
		}

		// Exit if client creation failed
		if (client == null) {
			LOGGER.error("Error occured while initializing the client");
			System.exit(0);
		}

		// Else, generate commands
		final CommandGenerator generator = new CommandGenerator();
		try {
			generator.generateCommands(Integer.parseInt(AppConfig.getValue("client.numberOfCommands")));
		} catch (NumberFormatException | IOException e) {
			// Exit if commands cannot be generated
			LOGGER.error("Error occured while generating the commands", e);
			System.exit(0);
		}

		// Execute these commands
		client.executeCommands(AppConfig.getValue("client.commandsFile"));		
	}

}
