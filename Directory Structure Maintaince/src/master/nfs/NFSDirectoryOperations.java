package master.nfs;

import java.util.InvalidPropertiesFormatException;

import org.apache.log4j.Logger;

import com.sun.media.sound.InvalidDataException;
import commons.Message;
import commons.dir.Directory;
import commons.dir.ICommandOperations;

public class NFSDirectoryOperations implements ICommandOperations {
	private final static Logger		LOGGER		= Logger.getLogger(NFSDirectoryOperations.class);

	@Override
	public Message ls(final Directory root,
			final String filePath,
			final String... arguments)
					throws InvalidPropertiesFormatException,
					InvalidDataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message mkdir(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message touch(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message rmdir(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void rm(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public Message cd(final Directory root,
			final String filePath,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Directory releaseParentReadLocks(final Directory root,
			final String filePath) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message acquireReadLocks(Directory root, String filePath, String... arguments) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message acquireWriteLocks(Directory root, String filePath, String... arguments) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message releaseReadLocks(Directory root, String filePath, String... arguments) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message releaseWriteLocks(Directory root, String filePath, String... arguments) {
		// TODO Auto-generated method stub
		return null;
	}
		
}