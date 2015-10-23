package client.cache;

import java.util.HashMap;

import commons.dir.Directory;

import master.gfs.GFSDirectoryOperations;

public class Cache {
	static HashMap<String, CacheEntry>	cache	= new HashMap<>();

	public static Directory getFromCache(final String directoryPath) {
		if (cache.containsKey(directoryPath)) {
			final CacheEntry entry = cache.get(directoryPath);
			if (entry.getValid()) {

				// TODO: send call to server
				final Directory dirFromServer = GFSDirectoryOperations.lsWithCache(null, directoryPath, entry.getTimeStamp());
				if (dirFromServer.getName()
						.equals("")) {
					addToCache(directoryPath, entry);
					return entry.getDir();
				} else if (dirFromServer != null) {
					entry.setTimeStamp(dirFromServer.getModifiedTimeStamp());
					addToCache(directoryPath, entry);
					return dirFromServer;
				}
			}
		}

		// TODO: send call to server
		return null;
	}

	public static Boolean addToCache(final String directoryPath, final CacheEntry entry) {

		cache.put(directoryPath, entry);
		return true;
	}

	public static Boolean checkValidity(final String directoryPath) {
		if (cache.containsKey(directoryPath)) {
			final CacheEntry entry = cache.get(directoryPath);
			entry.setValid(false);
			cache.put(directoryPath, entry);
			return true;
		}

		return false;
	}
}