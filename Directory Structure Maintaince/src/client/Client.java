package client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import commons.AppConfig;
import commons.CommandsSupported;
import commons.Message;

/**
 * Class to implement the client interface
 */
public class Client {
	private final Socket				socket;
	private final ObjectInputStream		inputStream;
	private final ObjectOutputStream	outputStream;
	private final static Logger			LOGGER	= Logger.getLogger(Client.class);

	/**
	 * Constructor
	 *
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public Client() throws UnknownHostException, IOException {
		// Initialize configuration
		new AppConfig("conf");
		LOGGER.setLevel(Level.INFO);

		socket = new Socket(AppConfig.getValue("client.masterIp"), Integer.parseInt(AppConfig.getValue("client.masterPort")));
		outputStream = new ObjectOutputStream(socket.getOutputStream());
		inputStream = new ObjectInputStream(socket.getInputStream());
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
				// Send command to master
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
				LOGGER.info("Command " + number + " : " + command);
				LOGGER.info(reply + "\n");

				number++;
			}
		} catch (final IOException | ClassNotFoundException e) {
			LOGGER.error("Error occured while executing commands", e);
			System.exit(0);
		}
	}

	/**
	 * Client`s main method
	 *
	 * @param args
	 *            Command line arguments
	 */
	public static void main(final String[] args) {
		// Create client
		Client client = null;
		try {
			client = new Client();
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