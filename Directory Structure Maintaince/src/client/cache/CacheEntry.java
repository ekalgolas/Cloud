package client.cache;

import metadata.Directory;

public class CacheEntry {
	private Directory	dir;
	private boolean		valid;
	private long		timeStamp;

	/**
	 * Constructor
	 *
	 * @param dir
	 *            Directory
	 * @param valid
	 *            Is a valid entry?
	 * @param timeStamp
	 *            time stamp for entry
	 */
	public CacheEntry(final Directory dir, final Boolean valid, final Long timeStamp) {
		super();
		this.dir = dir;
		this.valid = valid;
		this.timeStamp = timeStamp;
	}

	/**
	 * @return the dir
	 */
	public Directory getDir() {
		return dir;
	}

	/**
	 * @param dir
	 *            the dir to set
	 */
	public void setDir(final Directory dir) {
		this.dir = dir;
	}

	/**
	 * @return the valid
	 */
	public Boolean getValid() {
		return valid;
	}

	/**
	 * @param valid
	 *            the valid to set
	 */
	public void setValid(final Boolean valid) {
		this.valid = valid;
	}

	/**
	 * @return the timeStamp
	 */
	public Long getTimeStamp() {
		return timeStamp;
	}

	/**
	 * @param timeStamp
	 *            the timeStamp to set
	 */
	public void setTimeStamp(final Long timeStamp) {
		this.timeStamp = timeStamp;
	}
}