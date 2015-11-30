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
	private static final long	DEFAULT_SIZE	= 0L;

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#ls(master.metadata.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public Message ls(Directory root,
			String filePath,
			final String... arguments)
			throws InvalidPropertiesFormatException,
			InvalidDataException {
		// Check if path is valid
		if (filePath.charAt(filePath.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should contain a '/' at the end");
		}

		filePath = filePath.substring(0, filePath.length() - 1);
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

		// Try acquiring read lock on the directory
		root.getReadLock()
			.lock();

		// True if detailed output asked for LS command (LSL)
		final boolean isDetailed = arguments != null && arguments[arguments.length - 1].equals("-l");

		// If we reach here, it means valid directory was found
		// Compute output
		final OutputFormatter output = new OutputFormatter();
		if (isDetailed) {
			output.addRow("TYPE", "NAME", "SIZE", "TIMESTAMP");
		} else {
			output.addRow("TYPE", "NAME");
		}

		// Append children
		for (final Directory child : root.getChildren()) {
			final String type = child.isFile() ? "File" : "Directory";
			if (isDetailed) {
				output.addRow(type, child.getName(), child.getSize()
					.toString(), child.getModifiedTimeStamp()
					.toString());
			} else {
				output.addRow(type, child.getName());
			}
		}

		// Release the read lock
		root.getReadLock()
			.unlock();

		// Return the representation
		return new Message("\n" + output.toString());
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
	protected Directory search(Directory root,
			final String filePath) {
		// Get list of paths
		final String[] paths = filePath.split("/");

		// Find the directory in directory tree
		for (int i = 0; i < paths.length; i++) {

			final String path = paths[i];
			// Match the root
			boolean found = false;
			if (root.getName().equals(path)) {
				found = true;
			}

			// Check if the path corresponds to any child in this directory
			for (final Directory child : root.getChildren()) {
				if (child.getName().equals(path)) {
					// Try acquiring read lock on the parent
					if (i != paths.length - 1) {
						child.getReadLock().lock();
					}

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
	 * Performs a tree search from the {@literal root} on the directory structure corresponding to the {@literal filePath} and releases all the read locks
	 *
	 * @param root
	 *            Root of directory structure
	 * @param filePath
	 *            Path to search
	 * @return Node corresponding to the path, null if not found
	 */
	@Override
	public Directory releaseParentReadLocks(Directory root,
			final String filePath) {
		// Get list of paths
		final String[] paths = filePath.split("/");

		// Find the directory in directory tree
		for (int i = 0; i < paths.length; i++) {

			final String path = paths[i];
			// Match the root
			boolean found = false;
			if (root.getName().equals(path)) {
				found = true;
			}

			// Check if the path corresponds to any child in this directory
			for (final Directory child : root.getChildren()) {
				if (child.getName().equals(path)) {
					if (i != paths.length - 1) {
						// Release the read lock on the parent
						if (child.isReadLocked()) {
							child.getReadLock().unlock();
						}
					}
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
	public Message mkdir(final Directory root,
			final String path,
			final String... arguments)
			throws InvalidPropertiesFormatException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should contain a '/' at the end");
		}

		// Get the parent directory and the name of directory
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = path.substring(0, path.length() - name.length() - 1);

		// Create the directory
		create(root, dirPath, name, false);
		final Message returnMessage = new Message("Directory Creation Succesful");
		return returnMessage;
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#touch(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public Message touch(final Directory root,
			final String path,
			final String... arguments)
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

		// Try acquiring write lock on the directory
		directory.getWriteLock()
			.lock();

		final List<Directory> contents = directory.getChildren();
		boolean found = false;
		for (final Directory child : contents) {
			if (child.getName()
				.equalsIgnoreCase(name)) {
				child.getWriteLock().lock();
				// Already present, set modified timestamp to current
				child.setModifiedTimeStamp(new Date().getTime());
				child.getWriteLock().unlock();
				found = true;
				break;
			}
		}
		if (!found) {
			// Not present, add it in the list
			final Directory file = new Directory(name, true, null, new Date().getTime(), DEFAULT_SIZE);
			contents.add(file);
		}
		directory.setChildren(contents);

		// Release write lock
		directory.getWriteLock()
			.unlock();

		return new Message("Touch Successful");
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

		final List<Directory> contents = directory.getChildren();
		for (final Directory child : contents) {
			if (child.getName()
				.equals(name)) {
				throw new InvalidPathException(path, "Path already present");
			}
		}

		// Try acquiring write lock on the directory
		if (!directory.getWriteLock().isHeldByCurrentThread()) {
			directory.getWriteLock().lock();
		}

		// Add file if isFile is true
		if (isFile) {
			final Directory file = new Directory(name, isFile, null, new Date().getTime(), DEFAULT_SIZE);
			contents.add(file);
		} else {
			// Else, add directory here
			final Directory dir = new Directory(name, isFile, new ArrayList<Directory>(), new Date().getTime(), DEFAULT_SIZE);
			contents.add(dir);
		}

		// Release the lock
		directory.getWriteLock().unlock();
		releaseParentReadLocks(root, path);
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#rmdir(master.metadata.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public Message rmdir(final Directory root,
			final String path,
			final String... arguments)
			throws InvalidPropertiesFormatException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should contain a '/' at the end");
		}

		// Get the parent directory and the name of directory
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = path.substring(0, path.length() - name.length() - 1);

		// True for RMDIRF i.e. rmdir -f option
		final boolean isForceRemove = arguments != null && arguments[arguments.length - 1].equals("-f");

		remove(root, dirPath, name, false, isForceRemove);
		return new Message("rmdir Successful");
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
			final boolean isFile,
			final boolean isForceRemove) {
		// Search and get to the directory where we want to remove
		final Directory directory = search(root, path);

		// If path was not found, throw exception
		if (directory == null) {
			releaseParentReadLocks(root, path);
			throw new InvalidPathException(path, "Path was not found");
		}

		// Try acquiring write lock on the directory
		directory.getWriteLock().lock();

		Directory directoryToRemove = null;
		final List<Directory> subDirectories = directory.getChildren();
		for (final Directory childDirectory : subDirectories) {
			if (childDirectory.getName().equals(name)) {
				if (childDirectory.isFile() != isFile) {
					// Release the lock
					directory.getWriteLock().unlock();
					final String message = isFile ? "Provided argument is a file, directory expected" : "Provided argument is a directory, file expected";
					releaseParentReadLocks(root, path);
					throw new IllegalArgumentException(message);
				} else {
					directoryToRemove = childDirectory;
					break;
				}
			}
		}

		// Remove only if directory is empty or force removal is asked
		final boolean canRemove = isForceRemove || directoryToRemove != null && directoryToRemove.isEmptyDirectory();
		if (canRemove) {
			subDirectories.remove(directoryToRemove);
		} else if (directoryToRemove == null) {
			// Release the lock
			directory.getWriteLock().unlock();
			releaseParentReadLocks(root, path);
			throw new IllegalStateException("Directory does not exist.");
		} else {
			// Release the lock
			directory.getWriteLock().unlock();
			releaseParentReadLocks(root, path);
			throw new IllegalStateException("Directory is not empty. Cannot remove.");
		}

		// Release the lock
		directory.getWriteLock().unlock();
		releaseParentReadLocks(root, path);
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#rm(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public void rm(final Directory root,
			final String path,
			final String... arguments)
			throws InvalidPropertiesFormatException {

	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#cd(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public Message cd(final Directory root,
			String filePath,
			final String... arguments)
			throws InvalidPropertiesFormatException {
		// Check if path is valid
		if (filePath.charAt(filePath.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should contain a '/' at the end");
		}

		filePath = filePath.substring(0, filePath.length() - 1);
		final Directory directory = search(root, filePath);

		// If search returns null, return
		if (directory == null) {
			throw new InvalidPathException(filePath, "Does not exist");
		}

		// If path is a file, return
		if (directory.isFile()) {
			throw new InvalidPropertiesFormatException(filePath + " is a file. Expecting directory!");
		}

		return new Message(String.valueOf(true));
	}

	@Override
	public Message acquireReadLocks(final Directory root,
			final String filePath,
			final String... arguments) {
		return null;
	}

	@Override
	public Message acquireWriteLocks(final Directory root,
			final String filePath,
			final String... arguments) {
		return null;
	}

	@Override
	public Message releaseReadLocks(final Directory root,
			final String filePath,
			final String... arguments) {
		return null;
	}

	@Override
	public Message releaseWriteLocks(final Directory root,
			final String filePath,
			final String... arguments) {
		return null;
	}
}