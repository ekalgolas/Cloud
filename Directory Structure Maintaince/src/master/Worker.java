package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import master.gfs.DirectoryOperations;
import master.gfs.Globals;

import commons.Message;

/**
 * <pre>
 * Worker class that serves the client
 * 	1. Given a socket connection, read the client request
 * 	2. Modify the metadata accordingly
 * </pre>
 *
 */
public class Worker implements Runnable {
	private static final String	LS			= "ls";
	private static final String	MKDIR		= "mkdir";
	private static final String	RMDIR		= "rmdir";
	private static final String	TOUCH		= "touch";

	public volatile boolean		isRunning	= true;
	private final Socket		workerSocket;
	private ObjectInputStream	inputStream;
	private ObjectOutputStream	outputStream;

	/**
	 * Constructor
	 *
	 * @param socket
	 *            Socket to get streams from
	 */
	public Worker(final Socket socket) {
		workerSocket = socket;

		// Initialize input and output streams
		try {
			outputStream = new ObjectOutputStream(workerSocket.getOutputStream());
			inputStream = new ObjectInputStream(workerSocket.getInputStream());
		} catch (final IOException e) {
			e.printStackTrace();
			isRunning = false;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// Read from input stream and then process the commands
		while (isRunning) {
			try {
				// Read the queried command
				final Message message = (Message) inputStream.readObject();
				final String command = message.getContent();
				String reply = "";

				// Figure out the command and call the operations
				if (command.startsWith(LS)) {
					// Command line parameter (directory name) start from index '3' in the received string
					reply = DirectoryOperations.ls(Globals.gfsMetadataRoot, command.substring(3));
				} else if (command.startsWith(MKDIR)) {
					// Command line parameter (directory name) start from index '6' in the received string
					DirectoryOperations.mkdir(Globals.gfsMetadataRoot, command.substring(6));
					reply = "Directory created successfully";
				} else if (command.startsWith(TOUCH)) {
					// Command line parameter (directory name) start from index '6' in the received string
					DirectoryOperations.touch(Globals.gfsMetadataRoot, command.substring(6));
					reply = "File created successfully";
				} else if (command.startsWith(RMDIR)) {
					// Command line parameter (directory name) start from index '6' in the received string
					DirectoryOperations.rmdir(Globals.gfsMetadataRoot, command.substring(6));
					reply = "Directory deleted successfully";
				} else {
					// Else, invalid command
					reply = "Invalid command: " + command;
				}

				// Write reply to the socket output stream
				outputStream.writeObject(new Message(reply));
				outputStream.flush();
			} catch (final IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}