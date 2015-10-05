package client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import commons.AppConfig;
import commons.Message;

/**
 * Class to implement the client interface
 */
public class Client {
	private final Socket socket;
	private final ObjectInputStream inputStream;
	private final ObjectOutputStream outputStream;

	/**
	 * Constructor
	 *
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public Client() throws UnknownHostException, IOException {
		// Initialize configuration
		new AppConfig("conf");

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
		/**
		 * TODO : Change it to line below in future, to read commands from input file:
		 * Scanner scanner = new Scanner(new File(inputFileName))
		 */
		try (Scanner scanner = new Scanner(System.in)){
			while(scanner.hasNext()) {
				final String command = scanner.nextLine();
				// Send command to master
				outputStream.writeObject(new Message(command));
				outputStream.flush();

				// Wait and read the reply
				final Message message = (Message) inputStream.readObject();
				final String reply = message.getContent();
				System.out.println(reply);
			}
		} catch (final IOException | ClassNotFoundException e) {
			System.err.println("Error occured while executing commands");
			e.printStackTrace();
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
		Client client = null;
		try {
			client = new Client();
		} catch (final UnknownHostException e) {
			e.printStackTrace();
		} catch (final IOException e) {
			e.printStackTrace();
		}

		/**
		 * Client can read a previously generated file which has a thousands of commands
		 * It then sends the commands one by one to the master,
		 * after receiving reply for the previous one
		 * TODO : We need to generate this file separately with combination of random and fixed commands
		 */
		if(client == null) {
			System.err.println("Error occured while initializing the client");
			System.exit(0);
		}

		client.executeCommands(AppConfig.getValue("client.inputFile"));
	}
}