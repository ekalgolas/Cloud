package master.gfs;

import java.nio.file.InvalidPathException;
import java.util.Date;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import commons.dir.Directory;

public class GFSMetadataReplicationOperations extends GFSDirectoryOperations {
	/**
	 * Replicates result of mkdir command
	 *
	 * @param primaryRoot
	 *            Primary root
	 * @param replicaRoot
	 *            Replication root
	 * @param path
	 *            Path of the directory
	 * @throws InvalidPropertiesFormatException
	 * @throws CloneNotSupportedException
	 */
	public void replicateMkdir(final Directory primaryRoot,
			final Directory replicaRoot,
			final String path)
					throws InvalidPropertiesFormatException,
					CloneNotSupportedException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should contain a '/' at the end");
		}

		// Get the parent directory and the name of directory
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = path.substring(0, path.length() - name.length() - 1);

		final Directory createdDir = search(primaryRoot, path);
		if (createdDir == null) {
			throw new InvalidPathException(path, "Does not exist in primary metadata");
		}

		releaseParentReadLocks(primaryRoot, path);
		final Directory replicaParentDir = search(replicaRoot, dirPath);
		if (replicaParentDir == null) {
			throw new InvalidPathException(dirPath, "Does not exist in replica metadata");
		}

		replicaParentDir.getWriteLock().lock();
		final Directory dir = (Directory) createdDir.clone();
		replicaParentDir.getChildren().add(dir);

		replicaParentDir.getWriteLock().unlock();
		releaseParentReadLocks(replicaRoot, dirPath);
	}

	/**
	 * Replicates the rmdir command
	 *
	 * @param replicaRoot
	 *            Replication root
	 * @param path
	 *            Path of the directory
	 * @throws InvalidPropertiesFormatException
	 */
	public void replicateRmdir(final Directory replicaRoot,
			final String path, final boolean isForceRemove)
					throws InvalidPropertiesFormatException {
		// Check if path is valid
		if (path.charAt(path.length() - 1) != '/') {
			throw new InvalidPropertiesFormatException("Argument invalid: Path should contain a '/' at the end");
		}

		// Get the parent directory and the name of directory
		final String[] paths = path.split("/");
		final String name = paths[paths.length - 1];
		final String dirPath = path.substring(0, path.length() - name.length() - 1);

		// Search and get to the directory where we want to remove
		final Directory replicaParentDir = search(replicaRoot, dirPath);

		// If path was not found, throw exception
		if (replicaParentDir == null) {
			throw new InvalidPathException(dirPath, "Path was not found");
		}

		Directory directoryToRemove = null;
		replicaParentDir.getWriteLock().lock();
		final List<Directory> subDirectories = replicaParentDir.getChildren();
		for (final Directory childDirectory : subDirectories) {
			if (childDirectory.getName().equals(name)) {
				directoryToRemove = childDirectory;
				break;
			}
		}

		// Remove only if directory is empty or force removal is asked
		final boolean canRemove = isForceRemove || directoryToRemove != null && directoryToRemove.isEmptyDirectory();
		if (canRemove) {
			subDirectories.remove(directoryToRemove);
		}

		// Release the lock
		replicaParentDir.getWriteLock().unlock();
		releaseParentReadLocks(replicaRoot, dirPath);
	}

	/**
	 * Replicates result of touch command
	 *
	 * @param primaryRoot
	 *            Primary root
	 * @param replicaRoot
	 *            Replication root
	 * @param path
	 *            Path of the directory
	 * @throws InvalidPropertiesFormatException
	 * @throws CloneNotSupportedException
	 */
	public void replicateTouch(final Directory primaryRoot,
			final Directory replicaRoot,
			final String path)
					throws InvalidPropertiesFormatException,
					CloneNotSupportedException {
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
			releaseParentReadLocks(primaryRoot, path);
			throw new InvalidPathException(path, "Does not exist in primary metadata");
		}

		releaseParentReadLocks(primaryRoot, path);
		final Directory replicaParentDirectory = search(replicaRoot, dirPath);
		if (replicaParentDirectory == null) {
			releaseParentReadLocks(primaryRoot, path);
			throw new InvalidPathException(dirPath, "Does not exist in replica metadata");
		}

		replicaParentDirectory.getWriteLock().lock();
		final List<Directory> contents = replicaParentDirectory.getChildren();
		boolean found = false;
		for (final Directory child : contents) {
			if (child.getName().equals(touchedFile.getName())) {
				child.getWriteLock().lock();
				// Already present, set modified timestamp to current
				child.setModifiedTimeStamp(touchedFile.getModifiedTimeStamp());
				child.getWriteLock().unlock();
				found = true;
				break;
			}
		}

		if (!found) {
			// Not present, add it in the list
			final Directory replicaFile = (Directory) touchedFile.clone();
			touchedFile.setModifiedTimeStamp(new Date().getTime());
			contents.add(replicaFile);
		}

		replicaParentDirectory.setChildren(contents);
		replicaParentDirectory.getWriteLock().unlock();
	}
}