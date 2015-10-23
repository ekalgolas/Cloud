package master.gfs;

import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.Date;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import com.sun.media.sound.InvalidDataException;

import commons.Message;
import commons.OutputFormatter;
import commons.dir.Directory;
import commons.dir.ICommandOperations;

/**
 * Class to implement various directory master.metadata operations
 */
public class GFSDirectoryOperations implements ICommandOperations {
	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#ls(master.metadata.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public Message ls(Directory root,
			final String filePath,
			final String... arguments)
					throws InvalidPropertiesFormatException,
					InvalidDataException {
		root = search(root, filePath);

		// If search returns null, return
		if (root == null) {
			throw new InvalidPathException(filePath, "Does not exist");
		}

		// If path is a file, return
		if (root.isFile()) {
			throw new InvalidPropertiesFormatException(filePath + " is a file. Expecting directory!");
		}

		// Error out if directory is empty
		if (root.getChildren()
				.size() == 0) {
			throw new InvalidDataException("Directory is empty");
		}

		// If we reach here, it means valid directory was found
		// Compute output
		final OutputFormatter output = new OutputFormatter();
		output.addRow("TYPE", "NAME");

		// Append children
		for (final Directory child : root.getChildren()) {
			final String type = child.isFile() ? "File" : "Directory";
			output.addRow(type, child.getName());
		}

		// Return the representation
		return new Message("\n" + output.toString());
	}

	/**
	 * @param root
	 *            - Root of the directory
	 * @param filePath
	 *            - path to get
	 * @param clientCacheTimestamp
	 *            - the latest timestamp of the client's client.cache
	 * @return a directory with no name if the client client.cache is valid, else if it exists return the directory, null otherwise
	 */
	public static Directory lsWithCache(Directory root,
			final String filePath,
			final Long clientCacheTimestamp) {
		// Get list of paths
		final String[] paths = filePath.split("/");

		// Find the directory in directory tree
		for (final String path : paths) {

			// Check if the path corresponds to any child in this directory
			boolean found = false;
			for (final Directory child : root.getChildren()) {
				if (child.getName()
						.equalsIgnoreCase(path)) {
					root = child;
					found = true;
					break;
				} else if (child.getModifiedTimeStamp() <= clientCacheTimestamp) {
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

	/**
	 * Performs a tree search from the {@literal root} on the directory structure corresponding to the {@literal filePath}
	 *
	 * @param root
	 *            Root of directory structure
	 * @param filePath
	 *            Path to search
	 * @return Node corresponding to the path, null if not found
	 */
	private Directory search(Directory root,
			final String filePath) {
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

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#mkdir(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public void mkdir(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
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

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#touch(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public void touch(final Directory root,
			final String path)
					throws InvalidPropertiesFormatException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) == '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should not contain a '/' at the end");
		}

		// Get the parent directory and the name of file
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = path.substring(0, path.length() - name.length());

		final Directory directory = search(root, dirPath);
		if (directory == null) {
			throw new InvalidPathException(dirPath, "Does not exist");
		}

		// Create the file
		final Directory file = new Directory(name, true, null);
		final List<Directory> contents = directory.getChildren();
		boolean found = false;
		for (final Directory child : contents) {
			if (child.equals(file)) {
				// Already present, set modified timestamp to current
				child.setModifiedTimeStamp(new Date().getTime());
				found = true;
				break;
			}
		}
		if (!found) {
			// Not present, add it in the list
			file.setModifiedTimeStamp(new Date().getTime());
			contents.add(file);
		}
		directory.setChildren(contents);
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
	private void create(final Directory root,
			final String path,
			final String name,
			final boolean isFile)
					throws InvalidPathException {
		// Search and get to the directory where we have to create
		final Directory directory = search(root, path);

		// If path was not found, throw exception
		if (directory == null) {
			throw new InvalidPathException(path, "Path was not found");
		}

		// Add file if isFile is true
		if (isFile) {
			final Directory file = new Directory(name, isFile, null);
			directory.getChildren()
			.add(file);
		} else {
			// Else, add directory here
			final Directory dir = new Directory(name, isFile, new ArrayList<Directory>());
			directory.getChildren()
			.add(dir);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#rmdir(master.metadata.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public void rmdir(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {

		// Check if path is valid
		if (path.charAt(path.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should contain a '/' at the end");
		}

		// Get the parent directory and the name of directory
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 2];
		final String dirPath = path.substring(0, path.length() - name.length() - 1);

		remove(root, dirPath, name, false);
	}

	/**
	 * Delete a resource in the directory tree
	 *
	 * @param root
	 *            Root of the directory structure to search in
	 * @param path
	 *            Path of the parent directory where the resource to be deleted resides
	 * @param name
	 *            Name of the resource
	 * @param isFile
	 *            Resource to be deleted is a file if true, directory otherwise
	 * @throws InvalidPathException
	 */
	private void remove(final Directory root,
			final String path,
			final String name,
			final boolean isFile) {
		// Search and get to the directory where we want to remove
		final Directory directory = search(root, path);

		// If path was not found, throw exception
		if (directory == null) {
			throw new InvalidPathException(path, "Path was not found");
		}

		Directory directoryToRemove = null;
		final List<Directory> subDirectories = directory.getChildren();
		for (final Directory childDirectory : subDirectories) {
			if (childDirectory.getName() == name) {
				if (childDirectory.isFile() != isFile) {
					final String message = isFile ? "Provided argument is a file, directory expected" : "Provided argument is a directory, file expected";
					throw new IllegalArgumentException(message);
				} else {
					directoryToRemove = childDirectory;
					break;
				}
			}
		}

		/**
		 * TODO : Currently we blindly remove the directory, but in future we may need to defer it saying directory is not empty. This will come into picture
		 * after finalizing the arguments we are supporting for rmdir.
		 */

		subDirectories.remove(directoryToRemove);
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#rm(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public void rm(final Directory root,
			final String path)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#cd(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public Message cd(final Directory root,
			final String filePath)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}
}