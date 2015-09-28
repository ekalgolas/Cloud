package cache;

import java.util.HashMap;

import master.Directory;
import master.DirectoryOperations;


public class Cache {

	static HashMap<String, CacheEntry> cache = new HashMap<>();
	
	
	public static Directory getFromCache(String directoryPath){
		if(cache.containsKey(directoryPath)){
			CacheEntry entry = cache.get(directoryPath);
			if(entry.valid){
				
				//TODO: send call to server
				Directory dirFromServer = DirectoryOperations.lsWithCache(null, directoryPath, entry.timeStamp);
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
	
	public static Boolean addToCache(String directoryPath, CacheEntry entry){
		
		cache.put(directoryPath, entry);
		return true;
	}
	
	public static Boolean checkValidity(String directoryPath){
		if(cache.containsKey(directoryPath)){
			CacheEntry entry = cache.get(directoryPath);
			entry.valid = false;
			cache.put(directoryPath, entry);
			return true;
		}
		
		return false;
	}
	
	
}
