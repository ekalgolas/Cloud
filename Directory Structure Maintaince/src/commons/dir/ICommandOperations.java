package commons.dir;

import java.util.InvalidPropertiesFormatException;

import commons.Message;

import metadata.Directory;

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
	 */
	public Message ls(Directory root, final String filePath, String... arguments) throws InvalidPropertiesFormatException;

	/**
	 * Create directory operation
	 *
	 * @param root
	 *            Root of the directory structure to search the path in
	 * @param path
	 *            Absolute path of the directory to be created
	 * @throws InvalidPropertiesFormatException
	 */
	public void mkdir(final Directory root, final String path, String... arguments) throws InvalidPropertiesFormatException;

	/**
	 * Create file operation
	 *
	 * @param root
	 *            Root of the directory structure to search the path in
	 * @param path
	 *            Absolute path of the file to be created
	 * @throws InvalidPropertiesFormatException
	 */
	public void touch(final Directory root, final String path) throws InvalidPropertiesFormatException;

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
	public void rmdir(final Directory root, final String path, String... arguments) throws InvalidPropertiesFormatException;

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
	public void rm(final Directory root, final String path) throws InvalidPropertiesFormatException;

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
	public Message cd(Directory root, final String filePath) throws InvalidPropertiesFormatException;
}