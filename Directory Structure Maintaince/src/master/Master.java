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
	private static Long			currentInodeNumber;
	private static String		mdsServerId;

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
	 *
	 * @return mds server id
	 */
	public static String getMdsServerId() {
		return mdsServerId;
	}

	public static String getMdsServerIpAddress() {
		return AppConfig.getValue("mds." + mdsServerId + ".ip");
	}

	/**
	 * Get the current inode number for a new node.
	 *
	 * @return current inode number
	 */
	public static Long getInodeNumber() {
		final Long startInodeNumber = Long.valueOf(AppConfig.getValue("mds.inode.start"));
		final Long endInodeNumber = Long.valueOf(AppConfig.getValue("mds.inode.end"));
		if (currentInodeNumber <= endInodeNumber && currentInodeNumber >= startInodeNumber) {
			return currentInodeNumber++;
		}
		return new Long(-1);
	}

	private static void sendToMDS(final String mdsServerId,
			final String fileName) {
		try {
			System.out.println("Connecting to " + AppConfig.getValue("mds." + mdsServerId + ".ip"));
			final Socket initalSetupListener = new Socket(AppConfig.getValue("mds." + mdsServerId + ".ip"),
					Integer.parseInt(AppConfig.getValue("mds.initial.port")));
			final ObjectOutputStream outputStream = new ObjectOutputStream(initalSetupListener.getOutputStream());
			outputStream.writeObject(MetadataManager.deserializeObject(fileName + ".img"));
			outputStream.flush();
			initalSetupListener.close();
		} catch (final IOException ioexp) {
			LOGGER.error(new Message(ioexp.getLocalizedMessage(), "", CompletionStatusCode.ERROR.name()));
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
		final String serverType = AppConfig.getValue("server.type");

		try {
			LOGGER.debug("Master Started");
			
			// Generate metadata for existing directory structure and et the globals root
			if(serverType.equalsIgnoreCase(Globals.GFS_MODE)) {
				final Directory directory = MetadataManager.generateGFSMetadata();
				Globals.gfsMetadataRoot = directory;
				// Create metadata replica and set global replica
				final Directory replica = MetadataManager.generateGFSMetadata();
				Globals.gfsMetadataCopy = replica;
			}
			else if(serverType.equalsIgnoreCase(Globals.NFS_MODE)) {
				final HashMap<String, File> fileMap = MetadataManager.generateNFSMetadata();
				Globals.nfsFileMap = fileMap;
			}
			else if(serverType.equalsIgnoreCase(Globals.MDS_MODE)) {
				LOGGER.debug("Generating MDS Metadata");
				generateMDSMetadata();
			}
			System.out.println("Done");
		} catch (ClassNotFoundException | IOException e) {
			LOGGER.error("", e);
		}

		Thread gfsListenerThread = null;
		Thread mdsListenerThread = null;
		Thread nfsListenerThread = null;

		// Launch listener to process input requests
		if (serverType.equalsIgnoreCase(Globals.GFS_MODE)) {
			final Listener gfsListener = new Listener(gfsListenerSocket,
					Globals.GFS_MODE);
			gfsListenerThread = new Thread(gfsListener);
			gfsListenerThread.start();
		} else if (serverType.equalsIgnoreCase(Globals.MDS_MODE)) {
			final Listener mdsListener = new Listener(mdsListenerSocket,
					Globals.MDS_MODE);
			mdsListenerThread = new Thread(mdsListener);
			mdsListenerThread.start();
		} else if (serverType.equalsIgnoreCase(Globals.NFS_MODE)) {
			final Listener nfsListener = new Listener(nfsListenerSocket,
					Globals.NFS_MODE);
			nfsListenerThread = new Thread(nfsListener);
			nfsListenerThread.start();
		}

		// Wait for listener thread to finish
		try {
			if (serverType.equalsIgnoreCase(Globals.GFS_MODE)) {
				gfsListenerThread.join();
			} else if (serverType.equalsIgnoreCase(Globals.MDS_MODE)) {
				mdsListenerThread.join();
			} else if (serverType.equalsIgnoreCase(Globals.NFS_MODE)) {
				nfsListenerThread.join();
			}
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.error("", e);
		}
	}

	/**
	 * Generates MDS metadata for Ceph
	 *
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private static void generateMDSMetadata()
			throws IOException,
			ClassNotFoundException {
		if (Globals.OVERALL_INITIATOR_MDS.equals(mdsServerId)) {
			LOGGER.debug("Initial MDS");
			final HashMap<String, String> clusterMap = new HashMap<>();
			final HashMap<String, ArrayList<String>> primaryToReplicaMap = new HashMap<>();
			MetadataManager.parseClusterMapDetails(clusterMap, primaryToReplicaMap);
			boolean allSerializedAvailable = false;
			for(final String primaryMds:primaryToReplicaMap.keySet())
			{
				File serFile = new File("data/"+primaryMds+".img");
				allSerializedAvailable &= serFile.exists();
			}
			if(!allSerializedAvailable)
			{
				MetadataManager.generateOverallCephPartition();
				sendToMDS("MDS2", "MDS2");
			}
			Globals.subTreePartitionList = (HashMap<String, Directory>) MetadataManager.deserializeObject("MDS1.img");
			if(!allSerializedAvailable)
			{
				MetadataManager.populateInodeDetails();
				MetadataManager.serializeObject(Globals.subTreePartitionList, mdsServerId);			
				if (primaryToReplicaMap.get(mdsServerId) != null) {
					for (final String replicaName : primaryToReplicaMap.get(mdsServerId)) {
						sendToMDS(replicaName, mdsServerId);
					}
				}
			}
		} else if (mdsServerId.startsWith(Globals.MDS_SERVER_ID_START)) {
			File serFile = new File("data/"+mdsServerId.toUpperCase()+".img");
			boolean serFileAvailable = serFile.exists();
			if(!serFileAvailable)
			{
				final ServerSocket initialReplicaLoad = new ServerSocket(Integer.parseInt(AppConfig.getValue("mds.initial.port")));
				final Socket primarySocket = initialReplicaLoad.accept();
				final ObjectInputStream inputStream = new ObjectInputStream(primarySocket.getInputStream());
				Globals.subTreePartitionList = (HashMap<String, Directory>) inputStream.readObject();
				initialReplicaLoad.close();
				MetadataManager.populateInodeDetails();
				MetadataManager.serializeObject(Globals.subTreePartitionList, mdsServerId);
				final HashMap<String, String> clusterMap = new HashMap<>();
				final HashMap<String, ArrayList<String>> primaryToReplicaMap = new HashMap<>();
				MetadataManager.parseClusterMapDetails(clusterMap, primaryToReplicaMap);
				if (primaryToReplicaMap.get(mdsServerId) != null) {
					for (final String replicaName : primaryToReplicaMap.get(mdsServerId)) {
						sendToMDS(replicaName, mdsServerId);
					}
				}
			}
			else
			{
				Globals.subTreePartitionList = (HashMap<String, Directory>) 
													MetadataManager.deserializeObject(
															mdsServerId.toUpperCase()+".img"); 
			}
		} 
		else 
		{
			File serFile = new File("data/MDS"+mdsServerId.charAt(mdsServerId.length()-1)+".img");
			boolean serFileAvailable = serFile.exists();
			if(!serFileAvailable)
			{	
				final ServerSocket initialReplicaLoad = new ServerSocket(Integer.parseInt(AppConfig.getValue("mds.initial.port")));
				final Socket primarySocket = initialReplicaLoad.accept();
				final ObjectInputStream inputStream = new ObjectInputStream(primarySocket.getInputStream());
				Globals.subTreePartitionList = (HashMap<String, Directory>) inputStream.readObject();
				initialReplicaLoad.close();
			}
			else
			{
				Globals.subTreePartitionList = (HashMap<String, Directory>) 
													MetadataManager.deserializeObject(
															"MDS"+mdsServerId.charAt(
															mdsServerId.length()-1)+".img");
			}
		}
	}
}