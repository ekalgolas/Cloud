package master.gfs;

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
	public static Directory mdsMetaDataRoot = null;
	
	public static final String GFS_SERVER_PORT = "gfs.server.port";
	public static final String MDS_SERVER_PORT = "mds.server.port";
}
