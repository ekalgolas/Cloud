package master;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.util.InvalidPropertiesFormatException;

import master.gfs.Directory;
import master.gfs.DirectoryParser;
import master.gfs.Globals;

import commons.AppConfig;

/**
 * <pre>
 * Class to implement the master server
 * Following is the functionality of the master :-
 * 	1. Read the existing deserialized data file of the existing directory structure
 * 	2. Serialize and create a global metadata object
 * 	3. Launch a {@link Listener} thread
 * 	4. Serve the client
 * </pre>
 */
public class Master {
	private static ServerSocket	listenerSocket	= null;

	/**
	 * Setup the listener socket
	 *
	 * @throws InvalidPropertiesFormatException
	 */
	public static void initializeMaster() throws InvalidPropertiesFormatException {
		// Initialize configuration
		new AppConfig("conf");

		// Do nothing if socket already initialized
		if (listenerSocket != null) {
			return;
		}

		// Else, initialize the socket
		try {
			listenerSocket = new ServerSocket(Integer.parseInt(AppConfig.getValue("server.port")));
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create in-memory metadata structure
	 *
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Directory generateMetadata() throws IOException, ClassNotFoundException {
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

			// Serialize and store the metadata
			storeMetadata(directory);
		}

		return directory;
	}

	/**
	 * Serialize the metadata and write it into the file
	 *
	 * @param directory
	 *            {@link Directory} object to be written
	 */
	public static void storeMetadata(final Directory directory) {
		final File metadataStore = new File(AppConfig.getValue("server.metadataFile"));
		try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(metadataStore))) {
			outputStream.writeObject(directory);
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Master`s main method
	 *
	 * @param args
	 *            Command line arguments
	 * @throws InvalidPropertiesFormatException
	 */
	public static void main(final String[] args) throws InvalidPropertiesFormatException {
		// Initialize master
		initializeMaster();

		// Generate metadata for existing directory structure
		try {
			final Directory directory = generateMetadata();
			// Set the globals root
			Globals.metadataRoot = directory;
		} catch (final ClassNotFoundException e) {
			e.printStackTrace();
		} catch (final IOException e) {
			e.printStackTrace();
		}

		// Launch listener to process input requests
		final Listener listener = new Listener(listenerSocket);
		final Thread listenerThread = new Thread(listener);
		listenerThread.start();

		// Wait for listener thread to finish
		try {
			listenerThread.join();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			e.printStackTrace();
		}
	}
}