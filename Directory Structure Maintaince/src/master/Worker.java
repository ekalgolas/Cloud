package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import master.ceph.CephDirectoryOperations;
import master.gfs.GFSDirectoryOperations;
import metadata.Directory;

import commons.CommandsSupported;
import commons.Globals;
import commons.Message;
import commons.dir.ICommandOperations;

/**
 * <pre>
 * Worker class that serves the client
 * 	1. Given a socket connection, read the client request
 * 	2. Modify the metadata accordingly
 * </pre>
 */
public class Worker implements Runnable {
	public volatile boolean		isRunning	= true;
	private final String		listenerType;
	private final Socket		workerSocket;
	private ObjectInputStream	inputStream;
	private ObjectOutputStream	outputStream;

	/**
	 * Constructor
	 *
	 * @param socket
	 *            Socket to get streams from
	 */
	public Worker(final Socket socket, final String listenerType) {
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
				final String command = message.getContent().toUpperCase();
				Message reply = null;
				Directory root = null;
				final StringBuffer partialFilePath = new StringBuffer();

				try {
					final ICommandOperations directoryOperations;

					// Figure out the command and call the operations
					if (Globals.GFS_MODE.equalsIgnoreCase(listenerType)) {
						directoryOperations = new GFSDirectoryOperations();
						root = Globals.gfsMetadataRoot;
					} else if (Globals.MDS_MODE.equalsIgnoreCase(listenerType)) {
						directoryOperations = new CephDirectoryOperations();
						root = Globals.findClosestNode(command.substring(3), partialFilePath);
					} else {
						directoryOperations = new GFSDirectoryOperations();
						root = Globals.gfsMetadataRoot;
					}

					if (command.startsWith(CommandsSupported.LS.name())) {
						// Command line parameter (directory name) start from index '3' in the received string
						reply = directoryOperations.ls(root, command.substring(3), partialFilePath.toString());
					} else if (command.startsWith(CommandsSupported.MKDIR.name())) {
						// Command line parameter (directory name) start from index '6' in the received string
						directoryOperations.mkdir(root, command.substring(6), partialFilePath.toString());
						reply = new Message("Directory created successfully");
					} else if (command.startsWith(CommandsSupported.TOUCH.name())) {
						// Command line parameter (directory name) start from index '6' in the received string
						directoryOperations.touch(root, command.substring(6));
						reply = new Message("File created successfully");
					} else if (command.startsWith(CommandsSupported.RMDIR.name())) {
						// Command line parameter (directory name) start from index '6' in the received string
						directoryOperations.rmdir(root, command.substring(6));
						reply = new Message("Directory deleted successfully");
					} else if (command.startsWith(CommandsSupported.EXIT.name())) {
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