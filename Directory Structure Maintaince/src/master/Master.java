package master;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.util.InvalidPropertiesFormatException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import commons.AppConfig;
import commons.Globals;
import commons.dir.Directory;
import commons.dir.DirectoryParser;

/**
 * <pre>
 * Class to implement the master server
 * Following is the functionality of the master :-
 * 	1. Read the existing deserialized data file of the existing directory structure
 * 	2. Serialize and create a global master.metadata object
 * 	3. Launch a {@link Listener} thread
 * 	4. Serve the client
 * </pre>
 */
public class Master {
	private static ServerSocket	gfsListenerSocket	= null;
	private static ServerSocket	mdsListenerSocket	= null;
	private final static Logger	LOGGER				= Logger.getLogger(Master.class);

	/**
	 * Setup the listener socket
	 *
	 * @throws InvalidPropertiesFormatException
	 */
	public static void initializeMaster()
			throws InvalidPropertiesFormatException {
		// Initialize configuration
		new AppConfig("conf");
		LOGGER.setLevel(Level.DEBUG);

		// Do nothing if socket already initialized
		if (gfsListenerSocket != null && mdsListenerSocket != null) {
			return;
		}

		// Else, initialize the socket
		try {
			if (gfsListenerSocket == null) {
				gfsListenerSocket = new ServerSocket(Integer.parseInt(AppConfig.getValue(Globals.GFS_SERVER_PORT)));
			}
			if (mdsListenerSocket == null) {
				mdsListenerSocket = new ServerSocket(Integer.parseInt(AppConfig.getValue(Globals.MDS_SERVER_PORT)));
			}
		} catch (final IOException e) {
			LOGGER.error("", e);
		}
	}

	/**
	 * Create in-memory master.metadata structure
	 *
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Directory generateMetadata()
			throws IOException,
			ClassNotFoundException {
		final File metadataStore = new File(AppConfig.getValue("server.metadataFile"));
		Directory directory = null;
		if (metadataStore.exists()) {
			// Read the file into an object
			try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(metadataStore))) {
				directory = (Directory) inputStream.readObject();
			}
		} else {
			// Parse the directory
			directory = DirectoryParser.parseText(AppConfig.getValue("server.inputFile"));

			// Serialize and store the master.metadata
			storeMetadata(directory);
		}

		return directory;
	}

	/**
	 * Serialize the master.metadata and write it into the file
	 *
	 * @param directory
	 *            {@link Directory} object to be written
	 */
	public static void storeMetadata(final Directory directory) {
		final File metadataStore = new File(AppConfig.getValue("server.metadataFile"));
		try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(metadataStore))) {
			outputStream.writeObject(directory);
		} catch (final IOException e) {
			LOGGER.error("", e);
		}
	}

	/**
	 * Master`s main method
	 *
	 * @param args
	 *            Command line arguments
	 * @throws InvalidPropertiesFormatException
	 */
	public static void main(final String[] args)
			throws InvalidPropertiesFormatException {
		// Initialize master
		initializeMaster();

		try {
			// Generate master.metadata for existing directory structure
			final Directory directory = generateMetadata();

			// Set the globals root
			Globals.gfsMetadataRoot = directory;
			
			// Create metadata replica
			final Directory replica = generateMetadata();
						
			// Set global replica
			Globals.gfsMetadataCopy = replica;

		} catch (ClassNotFoundException | IOException e) {
			LOGGER.error("", e);
		}

		// Launch listener to process input requests
		final Listener gfsListener = new Listener(gfsListenerSocket, Globals.GFS_MODE);
		final Thread gfsListenerThread = new Thread(gfsListener);
		gfsListenerThread.start();

		final Listener mdsListener = new Listener(mdsListenerSocket, Globals.MDS_MODE);
		final Thread mdsListenerThread = new Thread(mdsListener);
		mdsListenerThread.start();

		// Wait for listener thread to finish
		try {
			gfsListenerThread.join();
			mdsListenerThread.join();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.error("", e);
		}
	}
}