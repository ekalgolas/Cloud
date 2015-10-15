package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import master.ceph.CephDirectoryOperations;
import master.gfs.DirectoryOperations;
import commons.Globals;
import commons.ICommandOperations;
import commons.Message;

/**
 * <pre>
 * Worker class that serves the client
 * 	1. Given a socket connection, read the client request
 * 	2. Modify the metadata accordingly
 * </pre>
 */
public class Worker implements Runnable {
	private static final String	LS			= "ls";
	private static final String	MKDIR		= "mkdir";
	private static final String	RMDIR		= "rmdir";
	private static final String	TOUCH		= "touch";
	private static final String	EXIT		= "exit";

	public volatile boolean		isRunning	= true;
	private String 				listenerType;
	private final Socket		workerSocket;
	private ObjectInputStream	inputStream;
	private ObjectOutputStream	outputStream;

	/**
	 * Constructor
	 *
	 * @param socket
	 *            Socket to get streams from
	 */
	public Worker(final Socket socket, String listenerType) {
		workerSocket = socket;
		this.listenerType = listenerType;

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
				Message reply = null;

				try {
					final ICommandOperations directoryOperations;
					// Figure out the command and call the operations
					if(Globals.GFS_MODE.equalsIgnoreCase(listenerType))
						directoryOperations = new DirectoryOperations();
					else if(Globals.MDS_MODE.equalsIgnoreCase(listenerType))
						directoryOperations = new CephDirectoryOperations();
					else
						directoryOperations = new DirectoryOperations();
					
					if (command.startsWith(LS)) {
						// Command line parameter (directory name) start from index '3' in the received string
						reply = directoryOperations.ls(Globals.gfsMetadataRoot, command.substring(3));
					} else if (command.startsWith(MKDIR)) {
						// Command line parameter (directory name) start from index '6' in the received string
						directoryOperations.mkdir(Globals.gfsMetadataRoot, command.substring(6));
						reply = new Message("Directory created successfully");
					} else if (command.startsWith(TOUCH)) {
						// Command line parameter (directory name) start from index '6' in the received string
						directoryOperations.touch(Globals.gfsMetadataRoot, command.substring(6));
						reply = new Message("File created successfully");
					} else if (command.startsWith(RMDIR)) {
						// Command line parameter (directory name) start from index '6' in the received string
						directoryOperations.rmdir(Globals.gfsMetadataRoot, command.substring(6));
						reply = new Message("Directory deleted successfully");
					} else if (command.startsWith(EXIT)) {
						// Close the connection
						isRunning = false;
					} else {
						// Else, invalid command
						reply = new Message("Invalid command: " + command);
					}
				} catch (final Exception e) {
					// If any command threw errors, propagate the error to the client
					reply = new Message(e.getMessage());
				}

				// Write reply to the socket output stream
				outputStream.writeObject(reply);
				outputStream.flush();
			} catch (final IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}