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
	@Override
	public String ls(final Directory root, final String filePath, final String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mkdir(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void touch(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rmdir(final Directory root, final String path, final String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rm(final Directory root, final String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}
}