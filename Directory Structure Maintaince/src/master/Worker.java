package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.InvalidPropertiesFormatException;

import master.ceph.CephDirectoryOperations;
import master.dht.DhtDirectoryOperations;
import master.gfs.GFSDirectoryOperations;
import master.gfs.GFSMetadataReplicationOperations;
import master.metadata.MetaDataServerInfo;

import org.apache.log4j.Logger;

import com.sun.media.sound.InvalidDataException;
import commons.CommandsSupported;
import commons.Globals;
import commons.Message;
import commons.dir.Directory;
import commons.dir.ICommandOperations;

/**
 * <pre>
 * Worker class that serves the client
 * 	1. Given a socket connection, read the client request
 * 	2. Modify the master.metadata accordingly
 * </pre>
 */
public class Worker implements Runnable {
	public volatile boolean		isRunning	= true;
	private final static Logger	LOGGER		= Logger.getLogger(Worker.class);
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
			LOGGER.error("", e);
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
				Directory root = null, replica = null;
				final StringBuffer partialFilePath = new StringBuffer();
				try {
					final ICommandOperations directoryOperations;
					final GFSMetadataReplicationOperations replicationOperations;

					// Figure out the command and call the operations
					if (Globals.GFS_MODE.equalsIgnoreCase(listenerType)) {
						directoryOperations = new GFSDirectoryOperations();
						replicationOperations = new GFSMetadataReplicationOperations();
						root = Globals.gfsMetadataRoot;
						replica = Globals.gfsMetadataCopy;
					} else if (Globals.MDS_MODE.equalsIgnoreCase(listenerType)) {
						directoryOperations = new CephDirectoryOperations();
						replicationOperations = null;
						root = MetaDataServerInfo.findClosestNode(command.substring(3), partialFilePath);
					} else {
						directoryOperations = new DhtDirectoryOperations();
						replicationOperations = null;
					}

					reply = executeCommand(command, root, replica, partialFilePath, directoryOperations, replicationOperations);
				} catch (final Exception e) {
					// If any command threw errors, propagate the error to the client
					reply = new Message(e.getMessage());
				}

				// Write reply to the socket output stream
				outputStream.writeObject(reply);
				outputStream.flush();
			} catch (final IOException | ClassNotFoundException e) {
				LOGGER.error("", e);
			}
		}
	}

	/**
	 * Figures out which command to execute
	 *
	 * @param command
	 *            Command as string
	 * @param root
	 *            Root of directory structure
	 * @param replica
	 *            Root of replication structure
	 * @param partialFilePath
	 *            Partial file path from Ceph
	 * @param directoryOperations
	 *            Object for the operations of the directory solution
	 * @param replicationOperations
	 *            Object for the operations of the replication solution
	 * @return Message as the result
	 * @throws InvalidPropertiesFormatException
	 * @throws InvalidDataException
	 * @throws CloneNotSupportedException
	 */
	private Message executeCommand(final String command,
			final Directory root,
			final Directory replica,
			final StringBuffer partialFilePath,
			final ICommandOperations directoryOperations,
			final GFSMetadataReplicationOperations replicationOperations)
					throws InvalidPropertiesFormatException,
					InvalidDataException,
					CloneNotSupportedException {
		Message reply = null;
		if (command.startsWith(CommandsSupported.LS.name())) {
			// Command line parameter (directory name) start from index '3' in the received string
			reply = directoryOperations.ls(root, command.substring(3), partialFilePath.toString());
		} else if (command.startsWith(CommandsSupported.MKDIR.name())) {
			// Command line parameter (directory name) start from index '6' in the received string
			final String argument = command.substring(6);

			reply = directoryOperations.mkdir(root, argument, partialFilePath.toString());
			if(replicationOperations != null) {
				replicationOperations.replicateMkdir(root, replica, argument);
				reply.appendContent(" with replication");
			}

			LOGGER.debug("Directory structure after " + command);
			LOGGER.debug("\n" + root.toString());
		} else if (command.startsWith(CommandsSupported.TOUCH.name())) {
			// Command line parameter (directory name) start from index '6' in the received string
			final String argument = command.substring(6);

			directoryOperations.touch(root, argument);
			reply = new Message("File created successfully");
			if(replicationOperations != null) {
				replicationOperations.replicateTouch(root, replica, argument);
				reply.appendContent(" with replication");
			}

			LOGGER.debug("Directory structure after " + command);
			LOGGER.debug("\n" + root.toString());
		} else if (command.startsWith(CommandsSupported.RMDIR.name())) {
			// Command line parameter (directory name) start from index '6' in the received string
			final String argument = command.substring(6);

			directoryOperations.rmdir(root, argument);
			reply = new Message("Directory deleted successfully");
			if(replicationOperations != null) {
				replicationOperations.replicateRmdir(replica, argument);
				reply.appendContent(" with replication");
			}

			LOGGER.debug("Directory structure after " + command);
			LOGGER.debug("\n" + root.toString());
		} else if (command.startsWith(CommandsSupported.EXIT.name())) {
			// Close the connection
			isRunning = false;
		} else {
			// Else, invalid command
			reply = new Message("Invalid command: " + command);
		}

		return reply;
	}
}