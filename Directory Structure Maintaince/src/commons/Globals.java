package commons;

import java.io.File;
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
	public static Directory						gfsMetadataRoot			= null;
	public static Directory						gfsMetadataCopy			= null;
	public static HashMap<String, Directory>	subTreePartitionList	= null;
	public static HashMap<String, File>			dhtFileMap				= null;

	public static final String					GFS_SERVER_PORT			= "gfs.server.port";
	public static final String					MDS_SERVER_PORT			= "mds.server.port";
	public static final String					DHT_SERVER_PORT			= "dht.server.port";
	public static final String					CLIENT_MDS_MASTER_PORT	= "client.masterPort";

	public static final String					GFS_MODE				= "GFS";
	public static final String					MDS_MODE				= "MDS";
	public static final String					DHT_MODE				= "DHT";
	public static final String					ALIVE_STATUS			= "Alive";
	public static final String					DEAD_STATUS				= "Dead";
	public static final String					PRIMARY_MDS				= "Primary";
	public static final String					REPLICA_MDS				= "Replica";

	// File search status codes.
	public static final String					PATH_NOT_FOUND			= "PNF";
	public static final String					PARTIAL_PATH_FOUND		= "PPF";
	public static final String					PATH_FOUND				= "PF";
	
	public static final String 					OVERALL_INITIATOR_MDS	= "MDS1";
	public static final String 					MDS_SERVER_ID_START		= "MDS";
}