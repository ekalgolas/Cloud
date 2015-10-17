package dht.dhtfs.core.def;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;

/**
 * @author Yinzi Chen
 * @date May 6, 2014
 */
public class AbstractFileSystem implements IFileSystem {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * dht.dhtfs.core.def.IFileSystem#initialize(dht.dhtfs.core.Configuration)
	 */
	@Override
	public void initialize(Configuration conf) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#create(dht.dhtfs.core.DhtPath)
	 */
	@Override
	public IFile create(DhtPath path) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#open(dht.dhtfs.core.DhtPath)
	 */
	@Override
	public IFile open(DhtPath path) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#open(dht.dhtfs.core.DhtPath, int)
	 */
	@Override
	public IFile open(DhtPath path, int mode) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#delete(dht.dhtfs.core.DhtPath)
	 */
	@Override
	public void delete(DhtPath path) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#rename(dht.dhtfs.core.DhtPath,
	 * dht.dhtfs.core.DhtPath)
	 */
	@Override
	public void rename(DhtPath srcPath, DhtPath dstPath) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#copy(dht.dhtfs.core.DhtPath,
	 * dht.dhtfs.core.DhtPath)
	 */
	@Override
	public void copy(DhtPath srcPath, DhtPath dstPath) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#copyFromLocal(dht.dhtfs.core.DhtPath,
	 * dht.dhtfs.core.DhtPath)
	 */
	@Override
	public void copyFromLocal(DhtPath srcPath, DhtPath dstPath) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#copyToLocal(dht.dhtfs.core.DhtPath,
	 * dht.dhtfs.core.DhtPath)
	 */
	@Override
	public void copyToLocal(DhtPath srcPath, DhtPath dstPath) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#mkdir(dht.dhtfs.core.DhtPath)
	 */
	@Override
	public void mkdir(DhtPath path) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#rmdir(dht.dhtfs.core.DhtPath,
	 * boolean)
	 */
	@Override
	public void rmdir(DhtPath path, boolean recursive) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#listStatus(dht.dhtfs.core.DhtPath)
	 */
	@Override
	public void listStatus(DhtPath path) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#isDirectory(dht.dhtfs.core.DhtPath)
	 */
	@Override
	public boolean isDirectory(DhtPath path) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#isFile(dht.dhtfs.core.DhtPath)
	 */
	@Override
	public boolean isFile(DhtPath path) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#exists(dht.dhtfs.core.DhtPath)
	 */
	@Override
	public boolean exists(DhtPath path) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
