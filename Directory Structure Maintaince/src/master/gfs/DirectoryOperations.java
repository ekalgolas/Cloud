package master.gfs;

import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;

/**
 * Class to implement various directory metadata operations
 *
 * @author Ekal.Golas
 */
public class DirectoryOperations {
	/**
	 * List directory operation
	 *
	 * @param root
	 *            Root of the directory to search in
	 * @param filePath
	 *            Path of directory whose listing is to be displayed
	 * @return Directory contents in string representation
	 */
	public static String ls(Directory root, final String filePath) {
		root = search(root, filePath);

		// If search returns null, return
		if (root == null) {
			return filePath + " does not exist";
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
	 * Performs a tree search from the {@literal root} on the directory structure corresponding to the {@literal filePath}
	 *
	 * @param root
	 *            Root of directory structure
	 * @param filePath
	 *            Path to search
	 * @return Node corresponding to the path, null if not found
	 */
	private static Directory search(Directory root, final String filePath) {
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
				return null;
			}
		}

		// Return the node where the path was found
		return root;
	}

	/**
	 * Create directory operation
	 *
	 * @param root
	 *            Root of the directory structure to search the path in
	 * @param path
	 *            Absolute path of the directory to be created
	 * @throws InvalidPropertiesFormatException
	 */
	public static void mkdir(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should contain a '/' at the end");
		}

		// Get the parent directory and the name of directory
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 2];
		final String dirPath = path.substring(0, path.length() - name.length() - 1);

		// Create the directory
		create(root, dirPath, name, false);
	}

	/**
	 * Create file operation
	 *
	 * @param root
	 *            Root of the directory structure to search the path in
	 * @param path
	 *            Absolute path of the file to be created
	 * @throws InvalidPropertiesFormatException
	 */
	public static void touch(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) == '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should not contain a '/' at the end");
		}

		// Get the parent directory and the name of file
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = path.substring(0, path.length() - name.length());

		// Create the file
		create(root, dirPath, name, true);
	}

	/**
	 * Create a resource in the directory tree
	 *
	 * @param root
	 *            Root of the directory structure to search in
	 * @param path
	 *            Path of the parent directory where the resource needs to be created
	 * @param name
	 *            Name of the resource
	 * @param isFile
	 *            Will create file if true, directory otherwise
	 * @throws InvalidPathException
	 */
	private static void create(final Directory root, final String path, final String name, final boolean isFile) throws InvalidPathException {
		// Search and get to the directory where we have to create
		final Directory directory = search(root, path);

		// If path was not found, throw exception
		if (directory == null) {
			throw new InvalidPathException(path, "Path was not found");
		}

		// Add file if isFile is true
		if (isFile) {
			final Directory file = new Directory(name, isFile, null);
			directory.getChildren().add(file);
		} else {
			// Else, add directory here
			final Directory dir = new Directory(name, isFile, new ArrayList<Directory>());
			directory.getChildren().add(dir);
		}
	}
}