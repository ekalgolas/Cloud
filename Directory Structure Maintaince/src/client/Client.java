package client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import commons.Message;


/**
 * Class to implement the client interface
 */
public class Client {
	/**
	 * Can read these both values from a config file
	 */
	private static final int MASTER_PORT = 18000;
	private static final String MASTER_HOST = "localhost";
	private static final String INPUT_FILE_NAME = "./data/sampleInputs.txt";

	private final Socket socket;
	private final ObjectInputStream inputStream;
	private final ObjectOutputStream outputStream;

	public Client() throws UnknownHostException, IOException {
		socket = new Socket(MASTER_HOST, MASTER_PORT);
		outputStream = new ObjectOutputStream(socket.getOutputStream());
		inputStream = new ObjectInputStream(socket.getInputStream());
	}

	public void executeCommands(final String inputFileName) {

		/**
		 * TODO : Change it to line below in future, to read commands from input file:
		 * Scanner scanner = new Scanner(new File(inputFileName))
		 */
		try (Scanner scanner = new Scanner(System.in)){
			
			while(scanner.hasNext()) {
				String command = scanner.nextLine();
				outputStream.writeObject(new Message(command));
				outputStream.flush();
				System.out.println("Reading reply");
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

		client.executeCommands(INPUT_FILE_NAME);
	}
}