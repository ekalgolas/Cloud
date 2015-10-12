package client.cache;

import java.util.InvalidPropertiesFormatException;

import metadata.Directory;

import commons.ICommandOperations;

/**
 * Class to provide cache operation before client contacts the master
 *
 * @author Ekal.Golas
 */
public class CacheOperations implements ICommandOperations {
	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#ls(metadata.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public String ls(final Directory root, final String filePath, final String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#mkdir(metadata.Directory, java.lang.String)
	 */
	@Override
	public void mkdir(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#touch(metadata.Directory, java.lang.String)
	 */
	@Override
	public void touch(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#rmdir(metadata.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public void rmdir(final Directory root, final String path, final String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#rm(metadata.Directory, java.lang.String)
	 */
	@Override
	public void rm(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#cd(metadata.Directory, java.lang.String)
	 */
	@Override
	public String cd(final Directory root, final String filePath) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Prints the current working directory
	 *
	 * @param root
	 *            Root of the directory structure
	 * @return Current directory name as string
	 */
	public String pwd(final Directory root) {
		return null;
	}
}