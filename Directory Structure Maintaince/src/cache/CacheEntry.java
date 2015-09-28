package cache;

import master.gfs.Directory;

public class CacheEntry {
	Directory dir;
	Boolean valid;
	Long timeStamp;

	CacheEntry(){

	}

	CacheEntry(final Directory dir, final Boolean valid){
		this.dir = dir;
		this.valid = valid;
	}
}