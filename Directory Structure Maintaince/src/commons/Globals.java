package commons;

import java.util.HashMap;

import metadata.Directory;

/**
 * Class to keep all the globals like metadata structure
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
	public static final String	LS			= "ls";
	public static final String	MKDIR		= "mkdir";
	public static final String	RMDIR		= "rmdir";
	public static final String	TOUCH		= "touch";
	public static final String	EXIT		= "exit";
}
