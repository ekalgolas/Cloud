package cache;

import master.Directory;

public class CacheEntry {

	Directory dir;
	Boolean valid;
	Long timeStamp;
	
	CacheEntry(){
		
	}
	
	CacheEntry(Directory dir, Boolean valid){
		this.dir = dir;
		this.valid = valid;
	}
	
}
