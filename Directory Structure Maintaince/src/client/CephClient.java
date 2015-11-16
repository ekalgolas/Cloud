package client;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
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
	
	private final HashMap<String,List<MetaDataServerInfo>> cachedServers = new HashMap<>();
	private final static Logger			LOGGER	= Logger.getLogger(CephClient.class);
	
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
				mdsServerSocket = new Socket(serverInfo.getIpAddress(), 
						Integer.parseInt(AppConfig.getValue("client.masterPort")));
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
	
	/**
	 * Execute commands listed in a file
	 *
	 * @param inputFileName
	 *            File that contains the commands
	 */
	public void executeCommands(final String inputFileName) {
		int number = 0;

		// Read commands
		try (Scanner scanner = new Scanner(new File(inputFileName))) {
			while (scanner.hasNext()) {
				final String command = scanner.nextLine();				

				if("EXIT".equals(command))
					break;
				
				final String[] commandParse = command.split(" ");
				final StringBuffer partialFilePath = new StringBuffer();
				
				final List<MetaDataServerInfo> mdsServers 
								= MetaDataServerInfo.findClosestServer(command
								.substring(commandParse[0].length()+1), 
								partialFilePath, this.cachedServers);
				
				boolean isWrite =false;
				if(CommandsSupported.MKDIR.name().equalsIgnoreCase(commandParse[0].trim())
						|| CommandsSupported.RMDIR.name().equalsIgnoreCase(commandParse[0].trim())
						|| CommandsSupported.TOUCH.name().equalsIgnoreCase(commandParse[0].trim()))
				{
					isWrite = true;
				}
				final Socket mdsServerSocket = getRequiredMdsSocket(mdsServers, isWrite);
				// Send command to master
				
				ObjectInputStream inputStream 
							= new ObjectInputStream(mdsServerSocket.getInputStream());
				ObjectOutputStream outputStream
							= new ObjectOutputStream(mdsServerSocket.getOutputStream());
				outputStream.writeObject(new Message(command));
				outputStream.flush();

				// Exit if command is exit
				if (command.equalsIgnoreCase(CommandsSupported.EXIT.name())) {
					LOGGER.info("Command " + number + " : " + command);
					break;
				}

				// Wait and read the reply
				final Message message = (Message) inputStream.readObject();
				final String reply = message.getContent();
				final String header = message.getHeader();
				if(message.getCompletionCode() != null
						&& !"".equals(message.getCompletionCode().toString().trim())
						&& CompletionStatusCode.SUCCESS.name()
							.equals(message.getCompletionCode().toString().trim())
						&& header != null 
						&& !"".equals(header.trim()))
				{
					final List<MetaDataServerInfo> newMdsServers 
						= MetaDataServerInfo.fromStringToMetadata(header.trim());
					if(!newMdsServers.isEmpty())
					{
						this.cachedServers.put(command
								.substring(commandParse[0].length()+1)
								, newMdsServers);
					}
				}
				LOGGER.info("Command " + number + " : " + command);
				LOGGER.info(reply + "\n");

				number++;
				mdsServerSocket.close();
			}
		} catch (final IOException | ClassNotFoundException e) {
			LOGGER.error("Error occured while executing commands", e);
			System.exit(0);
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
