package master;

/**
 * Class to implement various directory metadata operations
 *
 *
 */
public class DirectoryOperations {
	public static String ls(Directory root, final String filePath) {
		// Get list of paths
		final String[] paths = filePath.split("/");

		// Find the directory in directory tree
		for (final String path : paths) {
			// Check if the path corresponds to any child in this directory
			boolean found = false;
			for (final Directory child : root.getChildren()) {
				if (child.getName().equalsIgnoreCase(path)) {
					root = child;
					found = true;
					break;
				}
			}

			// If child was not found, path does not exists
			if (!found) {
				return filePath + " does not exist";
			}
		}

		// If path is a file, return
		if (root.isFile()) {
			return filePath + " is a file, please provide directory";
		}

		// If we reach here, it means valid directory was found
		// Compute output
		final StringBuilder builder = new StringBuilder("Listing for " + root.getName() + "\n");
		builder.append("TYPE\t\t\t\tNAME\n====\t\t\t\t====\n\n");

		// Append children
		for (final Directory child : root.getChildren()) {
			final String type = child.isFile() ? "File" : "Directory";
			builder.append(type + "\t\t\t\t" + child.getName());
		}

		// Return the representation
		return builder.toString();
	}
	
	
	/**
	 * 
	 * @param root - Root of the directory
	 * @param filePath - path to get
	 * @param clientCacheTimestamp - the latest timestamp of the client's cache
	 * @return a directory with no name if the client cache is valid, else if it exists return the directory, null otherwise 
	 */
	public static Directory lsWithCache(Directory root, final String filePath, Long clientCacheTimestamp){
		
		// Get list of paths
		final String[] paths = filePath.split("/");

		// Find the directory in directory tree
		for (final String path : paths) {
			
			// Check if the path corresponds to any child in this directory
			boolean found = false;
			for (final Directory child : root.getChildren()) {
				if (child.getName().equalsIgnoreCase(path)) {
					root = child;
					found = true;
					break;
				}else if(child.getModifiedTimeStamp() <= clientCacheTimestamp){
					return new Directory("", false, null);
				}
			}

			// If child was not found, path does not exists
			if (!found) {
				return null;
			}
		}
		
		return null;
	}
	
}