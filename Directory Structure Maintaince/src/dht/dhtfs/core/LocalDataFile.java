package dht.dhtfs.core;

import java.io.IOException;
import java.io.RandomAccessFile;

import dht.dhtfs.core.def.IFile;

/**
 * @author Yinzi Chen
 * @date Mar 27, 2014
 */
public class LocalDataFile implements IFile {
	private String fileName;
	private RandomAccessFile file;

	protected LocalDataFile(DhtPath path, int mode) throws IOException {
		String m = "r";
		if ((mode & WRITE) == WRITE) {
			m = "rw";
		}
		this.fileName = path.getAbsolutePath();
		this.file = new RandomAccessFile(fileName, m);
	}

	public static LocalDataFile create(DhtPath path) throws IOException {
		return new LocalDataFile(path, READ | WRITE);
	}

	public static LocalDataFile open(DhtPath path, int mode) throws IOException {
		return new LocalDataFile(path, mode);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#close()
	 */
	@Override
	public void close() throws IOException {
		file.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#setLength(long)
	 */
	@Override
	public void setLength(long newLength) throws IOException {
		file.setLength(newLength);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#getFilePointer()
	 */
	@Override
	public long getFilePointer() throws IOException {
		return file.getFilePointer();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#length()
	 */
	@Override
	public long length() throws IOException {
		return file.length();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#read(byte[])
	 */
	@Override
	public int read(byte[] b) throws IOException {
		return file.read(b);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#read(byte[], int, int)
	 */
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return file.read(b, off, len);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#write(byte[])
	 */
	@Override
	public void write(byte[] b) throws IOException {
		file.write(b);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#write(byte[], int, int)
	 */
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		file.write(b, off, len);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#seek(long)
	 */
	@Override
	public void seek(long pos) throws IOException {
		file.seek(pos);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#flush()
	 */
	@Override
	public void flush() throws IOException {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#insert(byte[])
	 */
	@Override
	public void insert(byte[] b) throws IOException {
		insert(b, (int) getFilePointer(), b.length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#insert(byte[], int, int)
	 */
	@Override
	public void insert(byte[] b, int off, int len) throws IOException {
		throw new IOException("insert is not implemented");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#checkSum()
	 */
	@Override
	public String checkSum() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
