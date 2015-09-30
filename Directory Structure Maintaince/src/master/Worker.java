package master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import master.gfs.DirectoryOperations;
import master.gfs.Globals;

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
	private BufferedReader		reader;
	private BufferedWriter		writer;

	/**
	 * Constructor
	 *
	 * @param socket
	 *            Socket to get streams from
	 */
	public Worker(final Socket socket) {
		workerSocket = socket;

		// Initialize reader and writer
		try {
			reader = new BufferedReader(new InputStreamReader(workerSocket.getInputStream()));
			writer = new BufferedWriter(new OutputStreamWriter(workerSocket.getOutputStream()));
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
				final String command = reader.readLine();
				String reply = "";

				// Figure out the command and call the operations
				if (command.startsWith(LS)) {
					// Command line parameter (directory name) start from index '3' in the received string
					reply = DirectoryOperations.ls(Globals.metadataRoot, command.substring(3));
				} else if (command.startsWith(MKDIR)) {
					// Command line parameter (directory name) start from index '6' in the received string
					DirectoryOperations.mkdir(Globals.metadataRoot, command.substring(6));
					reply = "Directory created successfully";
				} else if (command.startsWith(TOUCH)) {
					// Command line parameter (directory name) start from index '6' in the received string
					DirectoryOperations.touch(Globals.metadataRoot, command.substring(6));
					reply = "File created successfully";
				} else if (command.startsWith(RMDIR)) {
					// Command line parameter (directory name) start from index '6' in the received string
					DirectoryOperations.rmdir(Globals.metadataRoot, command.substring(6));
					reply = "Directory deleted successfully";
				} else {
					// Else, invalid command
					reply = "Invalid command: " + command;
				}

				// Write the output of the command to writer
				writer.write(reply);
				writer.flush();
			} catch (final IOException e) {
				e.printStackTrace();
			}
		}
	}
}