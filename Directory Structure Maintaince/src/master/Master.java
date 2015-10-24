package master;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;

import master.metadata.MetadataManager;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import commons.AppConfig;
import commons.Globals;
import commons.dir.Directory;

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
	private static ServerSocket	dhtListenerSocket	= null;
	private final static Logger	LOGGER				= Logger.getLogger(Master.class);

	/**
	 * Setup the listener socket
	 *
	 * @throws InvalidPropertiesFormatException
	 */
	public Master() throws InvalidPropertiesFormatException {
		// Initialize configuration
		new AppConfig("conf");
		LOGGER.setLevel(Level.DEBUG);

		// Do nothing if socket already initialized
		if (gfsListenerSocket != null && mdsListenerSocket != null && dhtListenerSocket != null) {
			return;
		}

		// Else, initialize the sockets
		try {
			if (gfsListenerSocket == null) {
				gfsListenerSocket = new ServerSocket(Integer.parseInt(AppConfig.getValue(Globals.GFS_SERVER_PORT)));
			}
			if (mdsListenerSocket == null) {
				mdsListenerSocket = new ServerSocket(Integer.parseInt(AppConfig.getValue(Globals.MDS_SERVER_PORT)));
			}
			if (dhtListenerSocket == null) {
				dhtListenerSocket = new ServerSocket(Integer.parseInt(AppConfig.getValue(Globals.DHT_SERVER_PORT)));
			}
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
		new Master();

		try {
			// Generate metadata for existing directory structure
			final Directory directory = MetadataManager.generateGFSMetadata();
			final HashMap<String, File> fileMap = MetadataManager.generateDHTMetadata();

			// Set the globals root
			Globals.gfsMetadataRoot = directory;
			Globals.dhtFileMap = fileMap;

			// Create metadata replica
			final Directory replica = MetadataManager.generateGFSMetadata();

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

		final Listener dhtListener = new Listener(dhtListenerSocket, Globals.DHT_MODE);
		final Thread dhtListenerThread = new Thread(dhtListener);
		dhtListenerThread.start();

		// Wait for listener thread to finish
		try {
			gfsListenerThread.join();
			mdsListenerThread.join();
			dhtListenerThread.join();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.error("", e);
		}
	}
}