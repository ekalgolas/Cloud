package dht.dhtfs.core.def;

/**
 * @author Yinzi Chen
 * @date May 5, 2014
 */
public interface ILockManager {

	public void acquireReadLock(String fileName);

	public void releaseReadLock(String fileName);

	public void acquireWriteLock(String fileName);

	public void releaseWriteLock(String fileName);
}
