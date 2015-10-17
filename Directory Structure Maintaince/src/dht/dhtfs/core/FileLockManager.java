package dht.dhtfs.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import dht.dhtfs.core.def.ILockManager;

/**
 * @author Yinzi Chen
 * @date May 5, 2014
 */
public class FileLockManager implements ILockManager {

	private Map<String, ReadWriteLock> metaLocks;

	public FileLockManager() {
		metaLocks = new HashMap<String, ReadWriteLock>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.ILockManager#acquireReadLock(java.lang.String)
	 */
	@Override
	public void acquireReadLock(String fileName) {
		ReadWriteLock fileLock;
		synchronized (metaLocks) {
			fileLock = metaLocks.get(fileName);
			if (fileLock == null) {
				fileLock = new ReentrantReadWriteLock();
				metaLocks.put(fileName, fileLock);
			}
		}
		Lock readLock = fileLock.readLock();
		readLock.lock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.ILockManager#releaseReadLock(java.lang.String)
	 */
	@Override
	public void releaseReadLock(String fileName) {
		ReadWriteLock fileLock;
		synchronized (metaLocks) {
			fileLock = metaLocks.get(fileName);
			if (fileLock == null) {
				fileLock = new ReentrantReadWriteLock();
				metaLocks.put(fileName, fileLock);
			}
		}
		Lock readLock = fileLock.readLock();
		readLock.unlock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.ILockManager#acquireWriteLock(java.lang.String)
	 */
	@Override
	public void acquireWriteLock(String fileName) {
		ReadWriteLock fileLock;
		synchronized (metaLocks) {
			fileLock = metaLocks.get(fileName);
			if (fileLock == null) {
				fileLock = new ReentrantReadWriteLock();
				metaLocks.put(fileName, fileLock);
			}
		}
		Lock writeLock = fileLock.writeLock();
		writeLock.lock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.ILockManager#releaseWriteLock(java.lang.String)
	 */
	@Override
	public void releaseWriteLock(String fileName) {
		ReadWriteLock fileLock;
		synchronized (metaLocks) {
			fileLock = metaLocks.get(fileName);
			if (fileLock == null) {
				fileLock = new ReentrantReadWriteLock();
				metaLocks.put(fileName, fileLock);
			}
		}
		Lock writeLock = fileLock.writeLock();
		writeLock.unlock();
	}

}
