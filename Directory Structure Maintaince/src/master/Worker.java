package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.InvalidPropertiesFormatException;

import master.ceph.CephDirectoryOperations;
import master.gfs.GFSDirectoryOperations;
import master.gfs.GFSMetadataReplicationOperations;
import master.metadata.MetaDataServerInfo;
import master.nfs.NFSDirectoryOperations;

import org.apache.log4j.Logger;

import com.sun.media.sound.InvalidDataException;

import commons.AppWatch;
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
	private static final AppWatch	APPWATCH	= new AppWatch();
	private final static Logger		LOGGER		= Logger.getLogger(Worker.class);
	private final String 	ACQUIRE_READ_LOCK 	= "ARLCK";
	private final String 	ACQUIRE_WRITE_LOCK	= "AWLCK";
	private final String 	RELEASE_READ_LOCK 	= "RRLCK";
	private final String 	RELEASE_WRITE_LOCK	= "RWLCK";
	public volatile boolean			isRunning	= true;
	private final String			listenerType;
	private final Socket			workerSocket;
	private ObjectInputStream		inputStream;
	private ObjectOutputStream		outputStream;	
	

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
				System.out.println("command:" + command);
				Message reply = null;
				Directory root = null;
				Directory replica = null;
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
						final String[] commandParse = command.split(" ");
						root = MetaDataServerInfo.findClosestNode(command.substring(commandParse[0].length() + 1),
								partialFilePath,
								Globals.subTreePartitionList);
					} else {
						directoryOperations = new NFSDirectoryOperations();
						replicationOperations = null;
					}

					APPWATCH.startWatch("Command execution started...");
					reply = executeCommand(command, root, replica, partialFilePath, directoryOperations, replicationOperations, message);

					final String performance = APPWATCH.stopAndLogTime("Command execution completed");
					reply.appendContent(performance);
				} catch (final Exception e) {
					// If any command threw errors, propagate the error to the client
					reply = new Message(e.getMessage() + " error occurred");
				}

				System.out.println(reply.toString());

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
			final GFSMetadataReplicationOperations replicationOperations,
			final Message message)
					throws InvalidPropertiesFormatException,
					InvalidDataException,
					CloneNotSupportedException {
		Message reply = null;
		String argument = "";
		try {
			if (command.startsWith(CommandsSupported.LSL.name())) {
				// Command line parameter (directory name) start from index '4'
				// in the received string
				argument = command.substring(4);

				reply = directoryOperations.ls(root, argument, partialFilePath.toString(), "-l");
			} else if (command.startsWith(CommandsSupported.LS.name())) {
				// Command line parameter (directory name) start from index '3'
				// in the received string
				argument = command.substring(3);

				reply = directoryOperations.ls(root, argument, partialFilePath.toString());
			} else if (command.startsWith(CommandsSupported.MKDIR.name())) {
				// Command line parameter (directory name) start from index '6'
				// in the received string
				argument = command.substring(6);

				reply = directoryOperations.mkdir(root, argument, partialFilePath.toString(), message.getHeader());
				if (replicationOperations != null) {
					replicationOperations.replicateMkdir(root, replica, argument);
				}

				logState(command, root);
			} else if (command.startsWith(CommandsSupported.TOUCH.name())) {
				// Command line parameter (directory name) start from index '6'
				// in the received string
				argument = command.substring(6);

				reply = directoryOperations.touch(root, argument, partialFilePath.toString(), message.getHeader());
				if (replicationOperations != null) {
					replicationOperations.replicateTouch(root, replica, argument);
				}

				logState(command, root);
			} else if (command.startsWith(CommandsSupported.RMDIRF.name())) {
				// Command line parameter (directory name) start from index '7'
				// in the received string
				argument = command.substring(7);

				reply = directoryOperations.rmdir(root, argument, 
						partialFilePath.toString(), message.getHeader(), "-f");
				if (replicationOperations != null) {
					replicationOperations.replicateRmdir(replica, argument);
				}

				logState(command, root);
			} else if (command.startsWith(CommandsSupported.RMDIR.name())) {
				// Command line parameter (directory name) start from index '6'
				// in the received string
				argument = command.substring(6);

				reply = directoryOperations.rmdir(root, argument, 
						partialFilePath.toString(), message.getHeader());
				if (replicationOperations != null) {
					replicationOperations.replicateRmdir(replica, argument);
				}

				logState(command, root);
			} else if (command.startsWith(CommandsSupported.CD.name())) {
				// Command line parameter (directory name) start from index '3'
				// in the received string
				argument = command.substring(3);

				reply = directoryOperations.cd(root, argument, partialFilePath.toString());

				logState(command, root);
			} 
			else if(command.startsWith(ACQUIRE_READ_LOCK))
			{
				// Command line parameter (directory name) start from index '5'
				// in the received string
				argument = command.substring(5);

				reply = directoryOperations.acquireReadLocks(root, 
									argument, 
									partialFilePath.toString());
			}
			else if(command.startsWith(ACQUIRE_WRITE_LOCK))
			{
				// Command line parameter (directory name) start from index '5'
				// in the received string
				argument = command.substring(5);

				reply = directoryOperations.acquireWriteLocks(root, 
									argument, 
									partialFilePath.toString());
			}
			else if(command.startsWith(RELEASE_READ_LOCK))
			{
				// Command line parameter (directory name) start from index '5'
				// in the received string
				argument = command.substring(5);

				reply = directoryOperations.releaseReadLocks(root, 
									argument, 
									partialFilePath.toString());
			}
			else if(command.startsWith(RELEASE_WRITE_LOCK))
			{
				// Command line parameter (directory name) start from index '5'
				// in the received string
				argument = command.substring(5);

				reply = directoryOperations.releaseWriteLocks(root, 
									argument, 
									partialFilePath.toString());
			}
			else if (command.startsWith(CommandsSupported.EXIT.name())) {
				// Close the connection
				isRunning = false;
			} else {
				// Else, invalid command
				reply = new Message("Invalid command: " + command);
			}
		} finally {
			directoryOperations.releaseParentReadLocks(root, argument);
		}

		return reply;
	}

	/**
	 * Logs the directory structure
	 *
	 * @param command
	 *            Command that changed structure
	 * @param root
	 *            Root of the directory structure
	 */
	private void logState(final String command,
			final Directory root) {
		if (root != null) {
			LOGGER.debug("Directory structure after " + command);
			LOGGER.debug("\n" + root.toString());
		}
	}
}