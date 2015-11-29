package master;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;

import master.metadata.MetadataManager;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import commons.AppConfig;
import commons.CompletionStatusCode;
import commons.Globals;
import commons.Message;
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
	private static ServerSocket	nfsListenerSocket	= null;
	private final static Logger	LOGGER				= Logger.getLogger(Master.class);
	private static Long currentInodeNumber;
	private static String mdsServerId;

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
		if (gfsListenerSocket != null && mdsListenerSocket != null && nfsListenerSocket != null) {
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
			if (nfsListenerSocket == null) {
				nfsListenerSocket = new ServerSocket(Integer.parseInt(AppConfig.getValue(Globals.NFS_SERVER_PORT)));
			}

			mdsServerId = AppConfig.getValue("mds.server.id");
		} catch (final IOException e) {
			LOGGER.error("", e);
		}

		currentInodeNumber = Long.valueOf(AppConfig.getValue("mds.current.inode"));
	}

	/**
	 * Get the server id for the mds.
	 * @return mds server id
	 */
	public static String getMdsServerId()
	{
		return mdsServerId;
	}

	public static String getMdsServerIpAddress()
	{
		return AppConfig.getValue("mds."+mdsServerId+".ip");
	}

	/**
	 * Get the current inode number for a new node.
	 * @return current inode number
	 */
	public static Long getInodeNumber()
	{
		final Long startInodeNumber = Long.valueOf(AppConfig.getValue("mds.inode.start"));
		final Long endInodeNumber = Long.valueOf(AppConfig.getValue("mds.inode.end"));
		if(currentInodeNumber <= endInodeNumber &&
				currentInodeNumber >=startInodeNumber)
		{
			return currentInodeNumber++;
		}
		return new Long(-1);
	}

	private static void sendToMDS(final String mdsServerId, final String fileName)
	{
		try
		{
			System.out.println("Connecting to "+AppConfig.getValue("mds."+mdsServerId+".ip"));
			final Socket initalSetupListener = new Socket(AppConfig.getValue("mds."+mdsServerId+".ip"),
					Integer.parseInt(AppConfig.getValue("mds.initial.port")));
			final ObjectOutputStream outputStream = new ObjectOutputStream(initalSetupListener.getOutputStream());
			outputStream.writeObject(MetadataManager.deserializeObject(fileName+".img"));
			outputStream.flush();
			initalSetupListener.close();
		}
		catch(final IOException ioexp)
		{
			LOGGER.error(new Message(ioexp.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name()));
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
			LOGGER.debug("Master Started");
			// Generate metadata for existing directory structure
			final Directory directory = MetadataManager.generateGFSMetadata();
			final HashMap<String, File> fileMap = MetadataManager.generateNFSMetadata();

			// Set the globals root
			Globals.gfsMetadataRoot = directory;
			Globals.nfsFileMap = fileMap;

			// Create metadata replica
			final Directory replica = MetadataManager.generateGFSMetadata();

			// Set global replica
			Globals.gfsMetadataCopy = replica;

			LOGGER.debug("Generating MDS Metadata");
			if(Globals.OVERALL_INITIATOR_MDS.equals(mdsServerId))
			{
				LOGGER.debug("Initial MDS");
				MetadataManager.generateOverallCephPartition();
				sendToMDS("MDS2","MDS2");
				sendToMDS("MDS3","MDS3");
				Globals.subTreePartitionList = (HashMap<String,Directory>) MetadataManager.deserializeObject("MDS1.img");
				MetadataManager.populateInodeDetails();
				MetadataManager.serializeObject(Globals.subTreePartitionList, mdsServerId);
				final HashMap<String, String> clusterMap = new HashMap<>();
				final HashMap<String, ArrayList<String>> primaryToReplicaMap = new HashMap<>();
				MetadataManager.parseClusterMapDetails(clusterMap, primaryToReplicaMap);
				if(primaryToReplicaMap.get(mdsServerId) != null)
				{
					for(final String replicaName:	primaryToReplicaMap.get(mdsServerId))
					{
						sendToMDS(replicaName,mdsServerId);
					}
				}
			}
			else if(mdsServerId.startsWith(Globals.MDS_SERVER_ID_START))
			{
				final ServerSocket initialReplicaLoad = new ServerSocket(Integer.parseInt(AppConfig.getValue("mds.initial.port")));
				final Socket primarySocket = initialReplicaLoad.accept();
				final ObjectInputStream inputStream = new ObjectInputStream(
						primarySocket.getInputStream());
				Globals.subTreePartitionList = (HashMap<String,Directory>)inputStream.readObject();
				initialReplicaLoad.close();
				MetadataManager.populateInodeDetails();
				MetadataManager.serializeObject(Globals.subTreePartitionList, mdsServerId);
				final HashMap<String, String> clusterMap = new HashMap<>();
				final HashMap<String, ArrayList<String>> primaryToReplicaMap = new HashMap<>();
				MetadataManager.parseClusterMapDetails(clusterMap, primaryToReplicaMap);
				if(primaryToReplicaMap.get(mdsServerId) != null)
				{
					for(final String replicaName:	primaryToReplicaMap.get(mdsServerId))
					{
						sendToMDS(replicaName,mdsServerId);
					}
				}
			}
			else
			{
				final ServerSocket initialReplicaLoad = new ServerSocket(Integer.parseInt(AppConfig.getValue("mds.initial.port")));
				final Socket primarySocket = initialReplicaLoad.accept();
				final ObjectInputStream inputStream = new ObjectInputStream(
						primarySocket.getInputStream());
				Globals.subTreePartitionList = (HashMap<String,Directory>)inputStream.readObject();
				initialReplicaLoad.close();
			}

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

		final Listener nfsListener = new Listener(nfsListenerSocket, Globals.NFS_MODE);
		final Thread nfsListenerThread = new Thread(nfsListener);
		nfsListenerThread.start();

		// Wait for listener thread to finish
		try {
			gfsListenerThread.join();
			mdsListenerThread.join();
			nfsListenerThread.join();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.error("", e);
		}
	}
}