package master.ceph;

import java.util.InvalidPropertiesFormatException;

import commons.ICommandOperations;
import metadata.Directory;

public class CephDirectoryOperations implements ICommandOperations {

	@Override
	public String ls(Directory root, String filePath, String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mkdir(Directory root, String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void touch(Directory root, String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rmdir(Directory root, String path, String... arguments) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rm(Directory root, String path) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public String cd(Directory root, String filePath) throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

}
