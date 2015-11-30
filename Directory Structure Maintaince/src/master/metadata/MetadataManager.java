package master.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import master.Master;
import master.nfs.NFSDirectoryParser;

import org.apache.log4j.Logger;

import commons.AppConfig;
import commons.CompletionStatusCode;
import commons.Globals;
import commons.Message;
import commons.dir.Directory;
import commons.dir.DirectoryParser;

/**
 * Class that manages the directory metadata
 *
 * @author Ekal.Golas
 */
public class MetadataManager {
	private final static Logger	LOGGER	= Logger.getLogger(MetadataManager.class);

	/**
	 * Create in-memory metadata structure for GFS
	 *
	 * @return Root of directory structure
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Directory generateGFSMetadata()
			throws IOException,
			ClassNotFoundException {
		final File metadataStore = new File(AppConfig.getValue("server.gfs.metadataFile"));
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
			storeGFSMetadata(directory);
		}

		return directory;
	}

	/**
	 * Create in-memory metadata structure for nfs
	 *
	 * @return Map of string to file
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static HashMap<String, File> generateNFSMetadata()
			throws IOException,
			ClassNotFoundException {
		HashMap<String, File> fileMap = null;

		// Parse the directory
		fileMap = NFSDirectoryParser.parseText(Integer.parseInt(AppConfig.getValue("server.nfs.cutLevel")));
		return fileMap;
	}

	/**
	 * Serialize the metadata and write it into the file
	 *
	 * @param directory
	 *            {@link Directory} object to be written
	 */
	public static void storeGFSMetadata(final Directory directory) {
		final File metadataStore = new File(AppConfig.getValue("server.gfs.metadataFile"));
		try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(metadataStore))) {
			outputStream.writeObject(directory);
		} catch (final IOException e) {
			LOGGER.error("", e);
		}
	}

	public static void serializeObject(final HashMap<String, Directory> partitionList,
			final String dataFileName)
	{
		try
		{
			final FileOutputStream fileOut = new FileOutputStream("data/" + dataFileName + ".img");
			final ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(partitionList);
			out.close();
			fileOut.close();
			// System.out.println("Serialized "+dataFileName);
		} catch (final IOException ioexp)
		{
			ioexp.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public static Object deserializeObject(final String dataFileName)
	{
		HashMap<String, Directory> mdsRoots = null;
		try
		{
			final FileInputStream fileIn = new FileInputStream("data/" + dataFileName);
			final ObjectInputStream in = new ObjectInputStream(fileIn);
			mdsRoots = (HashMap<String, Directory>) in.readObject();
			in.close();
			fileIn.close();
		} catch (final IOException i)
		{
			LOGGER.error(new Message(i.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name()));
			return null;
		} catch (final ClassNotFoundException c)
		{
			LOGGER.error(new Message(c.getLocalizedMessage(),
					"",
					CompletionStatusCode.ERROR.name()));
			return null;
		}
		return mdsRoots;
	}

	private static void traverseAndAssignInode(final Directory root,
			final HashMap<String, String> clusterMap,
			final HashMap<String, ArrayList<String>> primaryToReplicaMap)
	{
		if (root != null)
		{
			Inode inode = root.getInode();
			if (inode != null && inode.getInodeNumber() == -1)
			{
				inode.setInodeNumber(null);
				return;
			}
			else if (inode == null)
			{
				inode = new Inode();
				inode.setInodeNumber(Master.getInodeNumber());
				final ArrayList<MetaDataServerInfo> metaDataList = new ArrayList<>();
				final MetaDataServerInfo primaryMds = new MetaDataServerInfo();
				primaryMds.setServerName(Master.getMdsServerId());
				primaryMds.setIpAddress(clusterMap.get(Master.getMdsServerId()));
				primaryMds.setServerType(Globals.PRIMARY_MDS);
				primaryMds.setStatus(Globals.ALIVE_STATUS);
				metaDataList.add(primaryMds);
				if (primaryToReplicaMap.get(primaryMds.getServerName()) != null)
				{
					for (final String replicaName : primaryToReplicaMap.get(primaryMds.getServerName()))
					{
						final MetaDataServerInfo replicaServer = new MetaDataServerInfo();
						replicaServer.setServerName(replicaName);
						replicaServer.setIpAddress(clusterMap.get(replicaName));
						replicaServer.setServerType(Globals.REPLICA_MDS);
						replicaServer.setStatus(Globals.ALIVE_STATUS);
						metaDataList.add(replicaServer);
					}
				}
				inode.getDataServerInfo().addAll(metaDataList);
				root.setInode(inode);
			}
			for (final Directory child : root.getChildren())
			{
				traverseAndAssignInode(child, clusterMap, primaryToReplicaMap);
			}
		}
	}

	public static void populateInodeDetails()
	{
		final HashMap<String, String> clusterMap = new HashMap<>();
		final HashMap<String, ArrayList<String>> primaryToReplicaMap = new HashMap<>();
		parseClusterMapDetails(clusterMap, primaryToReplicaMap);

		for (final String path : Globals.subTreePartitionList.keySet())
		{
			traverseAndAssignInode(Globals.subTreePartitionList.get(path),
					clusterMap, primaryToReplicaMap);
		}
	}

	public static void parseClusterMapDetails(final HashMap<String, String> clusterMap,
			final HashMap<String, ArrayList<String>> primaryToReplicaMap)
	{
		final String clusterString = AppConfig.getValue("mds.replica.info");
		final String[] replicaSplit = clusterString.split("#");
		for (final String pair : replicaSplit)
		{
			final String[] pairValues = pair.split(":");
			clusterMap.put(pairValues[0], AppConfig.getValue("mds." + pairValues[0] + ".ip"));
			if (pairValues.length > 1)
			{
				final ArrayList<String> replicaList = new ArrayList<>();
				for (final String replicas : pairValues[1].split(","))
				{
					replicaList.add(replicas);
					clusterMap.put(replicas, AppConfig.getValue("mds." + replicas + ".ip"));
				}
				primaryToReplicaMap.put(pairValues[0], replicaList);
			}
		}
	}

	/**
	 * Generate the partitioned metadata required for the initial run of CEPH filesystem.
	 */
	public static void generateOverallCephPartition()
	{
		final Directory root = DirectoryParser.parseText(AppConfig.getValue("server.inputFile"));

		final HashMap<String, String> clusterMap = new HashMap<>();
		final HashMap<String, ArrayList<String>> primaryToReplicaMap = new HashMap<>();
		parseClusterMapDetails(clusterMap, primaryToReplicaMap);

		final HashMap<String, Directory> pathMap = new HashMap<String, Directory>();

		final HashMap<String, String> pathtoMdsMapping = new HashMap<>();
		final String mdsParititionSetting = AppConfig.getValue("mds.partition.config");
		final ArrayList<Directory> cutDirs = new ArrayList<Directory>();
		if (mdsParititionSetting != null &&
				!"".equals(mdsParititionSetting))
		{
			final String[] mdsPartitions = mdsParititionSetting.split(":");
			final ArrayList<String> paths = new ArrayList<String>();
			for (final String paritition : mdsPartitions)
			{
				final String[] mdsPathSplit = paritition.split("#");
				pathtoMdsMapping.put(mdsPathSplit[1], mdsPathSplit[0]);
				paths.add(mdsPathSplit[1]);
			}

			for (final String path : paths) {
				final StringBuffer partition = new StringBuffer();
				Directory currentDirNode = MetaDataServerInfo.findClosestNode(path, partition, pathMap);
				if (currentDirNode == null) {
					currentDirNode = root;
				}
				Directory parentDirNode = null;
				final String[] pathElem = path.split("/");
				for (int i = 1; i < pathElem.length; i++) {
					for (final Directory dir : currentDirNode.getChildren()) {
						if (dir.getName().equalsIgnoreCase(pathElem[i])) {
							parentDirNode = currentDirNode;
							currentDirNode = dir;
							break;
						}
					}
				}
				cutDirs.add(currentDirNode);
				pathMap.put(path, currentDirNode);
				parentDirNode.getChildren().remove(currentDirNode);
				final Directory childPointer = new Directory(currentDirNode.getName(),
						currentDirNode.isFile(), null);
				final Inode childInode = new Inode();
				final ArrayList<MetaDataServerInfo> metaDataList = new ArrayList<>();
				final MetaDataServerInfo primaryMds = new MetaDataServerInfo();
				primaryMds.setServerName(pathtoMdsMapping.get(path));
				primaryMds.setIpAddress(clusterMap.get(pathtoMdsMapping.get(path)));
				primaryMds.setServerType(Globals.PRIMARY_MDS);
				primaryMds.setStatus(Globals.ALIVE_STATUS);
				metaDataList.add(primaryMds);
				if (primaryToReplicaMap.get(primaryMds.getServerName()) != null)
				{
					for (final String replicaName : primaryToReplicaMap.get(primaryMds.getServerName()))
					{
						final MetaDataServerInfo replicaServer = new MetaDataServerInfo();
						replicaServer.setServerName(replicaName);
						replicaServer.setIpAddress(clusterMap.get(replicaName));
						replicaServer.setServerType(Globals.REPLICA_MDS);
						replicaServer.setStatus(Globals.ALIVE_STATUS);
						metaDataList.add(replicaServer);
					}
				}
				childInode.setInodeNumber(new Long(-1));
				childInode.getDataServerInfo().addAll(metaDataList);
				childPointer.setInode(childInode);
				parentDirNode.getChildren().add(childPointer);
			}

		}

		cutDirs.add(root);
		pathMap.put("root", root);
		pathtoMdsMapping.put("root", "MDS1");

		final HashMap<String, HashMap<String, Directory>> mdsPartitionDetails = new HashMap<>();

		for (final String pathName : pathtoMdsMapping.keySet())
		{
			if (!mdsPartitionDetails.containsKey(pathtoMdsMapping.get(pathName)))
			{
				mdsPartitionDetails.put(pathtoMdsMapping.get(pathName), new HashMap<>());
			}
			mdsPartitionDetails.get(pathtoMdsMapping.get(pathName))
				.put(pathName, pathMap.get(pathName));
		}

		for (final String mds : mdsPartitionDetails.keySet())
		{
			serializeObject(mdsPartitionDetails.get(mds), mds);
		}
	}
}