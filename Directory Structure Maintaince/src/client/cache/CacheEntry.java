package client.cache;

import metadata.Directory;

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