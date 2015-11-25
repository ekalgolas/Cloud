package commons.dir;

import java.util.InvalidPropertiesFormatException;

import com.sun.media.sound.InvalidDataException;

import commons.Message;

/**
 * Interface to provide directory command operations
 *
 * @author Ekal.Golas
 */
public interface ICommandOperations {
	/**
	 * List directory operation
	 *
	 * @param root
	 *            Root of the directory to search in
	 * @param filePath
	 *            Path of directory whose listing is to be displayed
	 * @param arguments
	 *            Arguments to the command
	 * @return Directory contents in string representation
	 * @throws InvalidPropertiesFormatException
	 * @throws InvalidDataException
	 */
	public Message ls(Directory root,
			final String filePath,
			String... arguments)
			throws InvalidPropertiesFormatException,
			InvalidDataException;

	/**
	 * Create directory operation
	 *
	 * @param root
	 *            Root of the directory structure to search the path in
	 * @param path
	 *            Absolute path of the directory to be created
	 * @throws InvalidPropertiesFormatException
	 */
	public Message mkdir(final Directory root,
			final String path,
			String... arguments)
			throws InvalidPropertiesFormatException;

	/**
	 * Create file operation
	 *
	 * @param root
	 *            Root of the directory structure to search the path in
	 * @param path
	 *            Absolute path of the file to be created
	 * @throws InvalidPropertiesFormatException
	 */
	public Message touch(final Directory root,
			final String path,
			String... arguments)
			throws InvalidPropertiesFormatException;

	/**
	 * Delete directory operation
	 *
	 * @param root
	 *            Root of the directory structure to search the path in
	 * @param path
	 *            Absolute path of the directory to be created
	 * @param arguments
	 *            Arguments to the command
	 * @throws InvalidPropertiesFormatException
	 */
	public Message rmdir(final Directory root,
			final String path,
			String... arguments)
			throws InvalidPropertiesFormatException;

	/**
	 * Delete file operation
	 *
	 * @param root
	 *            Root of the directory structure to search the path in
	 * @param path
	 *            Absolute path of the directory to be created
	 * @param arguments
	 *            Arguments to the command
	 * @throws InvalidPropertiesFormatException
	 */
	public void rm(final Directory root,
			final String path,
			String... arguments)
			throws InvalidPropertiesFormatException;

	/**
	 * Changes the current directory to the given path
	 *
	 * @param root
	 *            Root of the directory structure
	 * @param filePath
	 *            Path of the directory
	 * @return The current working directory after change
	 * @throws InvalidPropertiesFormatException
	 */
	public Message cd(Directory root,
			final String filePath,
			String... arguments)
			throws InvalidPropertiesFormatException;
	
	/**
     * Performs a tree search from the {@literal root} on the directory structure corresponding to the {@literal filePath}
     * and releases all the read locks
     * @param root
     *            Root of directory structure
     * @param filePath
     *            Path to search
     * @return Node corresponding to the path, null if not found
     */
    public Directory releaseParentReadLocks(Directory root,
            final String filePath);
    
    /**
     * Used to acquire read lock for the specified destination
     * @param root
     * @param filePath
     * @param arguments
     * @return status of acquiring read locks
     */
    public Message acquireReadLocks(final Directory root,
    		final String filePath,
    		final String... arguments);
   
    /**
     * Used to acquire write lock for the specified destination
     * @param root
     * @param filePath
     * @param arguments
     * @return status of acquiring write locks
     */
    public Message acquireWriteLocks(final Directory root,
    		final String filePath,
    		final String... arguments);
 
    /**
     * Used to release all the read locks for the specified destination
     * @param root
     * @param filePath
     * @param arguements
     * @return status of releasing the read locks
     */
    public Message releaseReadLocks(final Directory root,
    		final String filePath,
    		final String... arguements);
    
    /**
     * Used to release the write locks for the specified destination
     * @param root
     * @param filePath
     * @param arguements
     * @return status of releasing the write lock
     */
    public Message releaseWriteLocks(final Directory root,
    		final String filePath,
    		final String... arguements);
}