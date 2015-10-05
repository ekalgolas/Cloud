package client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

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
	private final BufferedReader reader;
	private final BufferedWriter writer;

	public Client() throws UnknownHostException, IOException {
		socket = new Socket(MASTER_HOST, MASTER_PORT);
		reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
	}

	public void executeCommands(final String inputFileName) {
		try (Scanner scanner = new Scanner(System.in)) {
			while (scanner.hasNext()) {
				final String command = scanner.nextLine();
				writer.write(command + "\n");
				writer.flush();

				final String reply = reader.readLine();
				System.out.println("Reply : " + reply);
			}
		} catch (final IOException e) {
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