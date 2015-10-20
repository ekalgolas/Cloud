package master.dht.dhtfs.core;

import java.util.concurrent.ConcurrentHashMap;

public class CachedFileMeta {
    private static CachedFileMeta cache;
    private ConcurrentHashMap<String, byte[]> fileMetas;

    public static CachedFileMeta getInstance() {
        if (cache == null) {
            synchronized (CachedFileMeta.class) {
                if (cache == null) {
                    cache = new CachedFileMeta();
                }
            }
        }
        return cache;
    }

    private CachedFileMeta() {
        fileMetas = new ConcurrentHashMap<String, byte[]>();
    }

    public byte[] get(String fileName) {
        return fileMetas.get(fileName);
    }

    public void update(String fileName, byte[] data) {
        fileMetas.put(fileName, data);
    }

    public void delete(String fileName) {
        fileMetas.remove(fileName);
    }
}
