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
	public static Directory	metadataRoot	= null;
}
