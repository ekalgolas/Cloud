package master.gfs;

import java.nio.file.InvalidPathException;
import java.util.Date;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import commons.dir.Directory;

public class GFSMetadataReplicationOperations {

	public void replicateMkdir(final Directory primaryRoot, 
			final Directory replicaRoot, 
			final String path) throws InvalidPropertiesFormatException, CloneNotSupportedException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException(
					"Argument invalid: Path should contain a '/' at the end");
		}

		// Get the parent directory and the name of directory
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 2];
		final String dirPath = path.substring(0, path.length() - name.length() - 1);

		final Directory createdDir = search(primaryRoot, path);
		if (createdDir == null) {
			throw new InvalidPathException(path, "Does not exist in primary metadata");
		}
		
		final Directory replicaParentDir = search(replicaRoot, dirPath);
		if (replicaParentDir == null) {
			throw new InvalidPathException(dirPath, "Does not exist in replica metadata");
		}

		final Directory dir = (Directory) createdDir.clone();
		replicaParentDir.getChildren().add(dir);
	}
	
	public void replicateRmdir(final Directory replicaRoot, 
			final String path) throws InvalidPropertiesFormatException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException(
					"Argument invalid: Path should contain a '/' at the end");
		}

		// Get the parent directory and the name of directory
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 2];
		final String dirPath = path.substring(0, path.length() - name.length() - 1);

		// Search and get to the directory where we want to remove
		final Directory replicaParentDir = search(replicaRoot, dirPath);

		// If path was not found, throw exception
		if (replicaParentDir == null) {
			throw new InvalidPathException(dirPath, "Path was not found");
		}

		Directory directoryToRemove = null;
		final List<Directory> subDirectories = replicaParentDir.getChildren();
		for (final Directory childDirectory : subDirectories) {
			if (childDirectory.getName().equals(name)) {
				directoryToRemove = childDirectory;
				break;
			}
		}
		subDirectories.remove(directoryToRemove);
	}
	
	public void replicateTouch(final Directory primaryRoot, 
			final Directory replicaRoot, 
			final String path) throws InvalidPropertiesFormatException, CloneNotSupportedException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) == '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should not contain a '/' at the end");
		}

		// Get the parent directory and the name of file
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = path.substring(0, path.length() - name.length());

		final Directory touchedFile = search(primaryRoot, path);
		if (touchedFile == null) {
			throw new InvalidPathException(path, "Does not exist in primary metadata");
		}
		
		final Directory replicaParentDirectory = search(replicaRoot, dirPath);
		if (replicaParentDirectory == null) {
			throw new InvalidPathException(dirPath, "Does not exist in replica metadata");
		}
		
		List<Directory> contents = replicaParentDirectory.getChildren();
		boolean found = false;
		for (final Directory child : contents) {
			if (child.getName().equals(touchedFile.getName())) {
				// Already present, set modified timestamp to current
				child.setModifiedTimeStamp(touchedFile.getModifiedTimeStamp());
				found = true;
				break;
			}
		}
		if (!found) {
			// Not present, add it in the list
			Directory replicaFile = (Directory) touchedFile.clone();
			touchedFile.setModifiedTimeStamp(new Date().getTime());
			contents.add(replicaFile);
		}
		replicaParentDirectory.setChildren(contents);
	}
	
	/**
	 * Performs a tree search from the {@literal root} on the directory structure corresponding to the {@literal filePath}
	 *
	 * @param root
	 *            Root of directory structure
	 * @param filePath
	 *            Path to search
	 * @return Node corresponding to the path, null if not found
	 */
	private Directory search(Directory root, final String filePath) {
		// Get list of paths
		final String[] paths = filePath.split("/");

		// Find the directory in directory tree
		for (final String path : paths) {
			// Match the root
			boolean found = false;
			if (root.getName()
					.equalsIgnoreCase(path)) {
				found = true;
			}

			// Check if the path corresponds to any child in this directory
			for (final Directory child : root.getChildren()) {
				if (child.getName()
						.equalsIgnoreCase(path)) {
					root = child;
					found = true;
					break;
				}
			}

			// If child was not found, path does not exists
			if (!found) {
				return null;
			}
		}

		// Return the node where the path was found
		return root;
	}
}
