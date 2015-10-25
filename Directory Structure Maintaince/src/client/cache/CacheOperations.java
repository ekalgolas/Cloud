package client.cache;

import java.util.InvalidPropertiesFormatException;

import commons.Message;
import commons.dir.Directory;
import commons.dir.ICommandOperations;

/**
 * Class to provide cache operation before client contacts the master
 *
 * @author Ekal.Golas
 */
public class CacheOperations implements ICommandOperations {
	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#ls(master.metadata.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public Message ls(final Directory root, final String filePath, final String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#mkdir(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public Message mkdir(final Directory root, final String path, String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#touch(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public Message touch(final Directory root, final String path, String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#rmdir(master.metadata.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public Message rmdir(final Directory root, final String path, final String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#rm(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public void rm(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * @see commons.ICommandOperations#cd(master.metadata.Directory, java.lang.String)
	 */
	@Override
	public Message cd(final Directory root, final String filePath) throws InvalidPropertiesFormatException {
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