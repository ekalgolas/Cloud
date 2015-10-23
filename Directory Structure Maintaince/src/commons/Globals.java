package commons;

import java.util.HashMap;

import commons.dir.Directory;

/**
 * Class to keep all the globals like master.metadata structure
 */
public class Globals {
	/**
	 * <pre>
	 * This is the handler for the directory structure
	 * Strictly use "synchronized" writes
	 * </pre>
	 */
	public static Directory	gfsMetadataRoot	= null;
	public static HashMap<String, Directory> subTreePartitionList = null;

	public static final String GFS_SERVER_PORT = "gfs.server.port";
	public static final String MDS_SERVER_PORT = "mds.server.port";

	public static final String GFS_MODE = "GFS";
	public static final String MDS_MODE = "MDS";
	public static final String ALIVE_STATUS = "Alive";
	public static final String DEAD_STATUS = "Dead";
	public static final String PRIMARY_MDS = "Primary";
	public static final String REPLICA_MDS = "Replica";

	// File search status codes.
	public static final String PATH_NOT_FOUND = "PNF";
	public static final String PARTIAL_PATH_FOUND = "PPF";
	public static final String PATH_FOUND = "PF";

	/**
	 * Find the closest Directory that matches with the required file path.
	 * @param filePath
	 * @return closest directory.
	 */
	public static Directory findClosestNode(final String filePath,final StringBuffer matchedPath)
	{
		int maxLevel = 0;
		String maxMatchPath = "";
		for(final String node:Globals.subTreePartitionList.keySet())
		{
			int currentLevel = 0;
			int i=0;
			while(i< node.length() && i < filePath.length())
			{
				if(node.charAt(i) == filePath.charAt(i))
				{
					if(node.charAt(i) == '/') {
						currentLevel++;
					}
				} else {
					break;
				}
				i++;
			}
			if(currentLevel > maxLevel && i==node.length())
			{
				maxLevel = currentLevel;
				maxMatchPath = node;
			}
		}
		matchedPath.append(maxMatchPath);
		return Globals.subTreePartitionList.get(maxMatchPath);
	}
}