package cache;

import java.util.HashMap;

import master.gfs.Directory;
import master.gfs.DirectoryOperations;


public class Cache {

	static HashMap<String, CacheEntry> cache = new HashMap<>();


	public static Directory getFromCache(final String directoryPath){
		if(cache.containsKey(directoryPath)){
			final CacheEntry entry = cache.get(directoryPath);
			if(entry.valid){

				//TODO: send call to server
				final Directory dirFromServer = DirectoryOperations.lsWithCache(null, directoryPath, entry.timeStamp);
				if(dirFromServer.getName().equals("")){
					addToCache(directoryPath,entry);
					return entry.dir;

				}
				else if(dirFromServer !=null){
					entry.timeStamp = dirFromServer.getModifiedTimeStamp();
					addToCache(directoryPath, entry);
					return dirFromServer;
				}
			}
		}

		//TODO: send call to server
		return null;
	}

	public static Boolean addToCache(final String directoryPath, final CacheEntry entry){

		cache.put(directoryPath, entry);
		return true;
	}

	public static Boolean checkValidity(final String directoryPath){
		if(cache.containsKey(directoryPath)){
			final CacheEntry entry = cache.get(directoryPath);
			entry.valid = false;
			cache.put(directoryPath, entry);
			return true;
		}

		return false;
	}


}
