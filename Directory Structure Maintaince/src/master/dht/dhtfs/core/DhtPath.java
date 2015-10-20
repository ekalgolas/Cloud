package master.dht.dhtfs.core;

import java.io.File;

import master.dht.dhtfs.core.def.IHashFunction;
import master.dht.dhtfs.server.datanode.DataServerConfiguration;

public class DhtPath extends File {

    private static final long serialVersionUID = 1L;
    private static IHashFunction hashFun = new MumurHash();

    public DhtPath(String path) {
        super(path);

        // Comment for Windows OS
        if (path.length() < 1 /* || path.charAt(0) != File.separatorChar */) {
            throw new IllegalArgumentException(
                    "path should be an absolute path, path: " + path);
        }
    }

    public String getHashFileName() {
        return getName();
    }

    public String getHashDirName() {
        return getName();
    }

    public DhtPath getDir() {
        return new DhtPath(super.getParent());
    }

    public DhtPath getMappingPath(String prefix) {
        return new DhtPath(prefix + fileNameHash(this.getAbsolutePath()));
    }

    public DhtPath getMappingPath(String prefix, String suffix) {
        return new DhtPath(getMappingPath(prefix).getAbsolutePath() + suffix);
    }

    private String fileNameHash(String fileName) {
        int val = hashFun.hashValue(fileName);
        // String hashName = DataRequestProcessor.dataDir + (val & 0xff) + "/" +
        // ((val & 0xff00) >> 8)
        // + "/" + ((val & 0xff0000) >> 16) + "/"
        // + (((val & 0xff000000) >> 24) & 0xff);

        // "/1024/1024/1024/512" for directory
        String hashName = "/" + (val & 0x3ff) + "/" + ((val & 0xffc00) >> 10)
                + "/" + ((val & 0x3ff00000) >> 20) + "/"
                + (((val & 0xff800000) >> 23) & 0x1ff);
        return hashName;
    }

    public String getDirKey() {
        String fullPath = this.getAbsolutePath();
        String[] path = fullPath.split("/");
        int mergeSize = DataServerConfiguration.getDirMergeSize();
        int idx = path.length - (path.length - 1) % mergeSize;
        StringBuilder key = new StringBuilder("");
        for (int i = 1; i < idx; ++i) {
            key.append("/" + path[i]);
        }
        if (key.length() == 0) {
            return "/";
        }
        return key.toString();
    }

    public String getParentKey() {
        String fullPath = this.getAbsolutePath();
        String[] path = fullPath.split("/");
        int mergeSize = DataServerConfiguration.getDirMergeSize();
        int idx = path.length - (path.length - 1) % mergeSize - mergeSize;
        if (idx < 0) {
            return null;
        }
        StringBuilder key = new StringBuilder("");
        for (int i = 1; i < idx; ++i) {
            key.append("/" + path[i]);
        }
        if (key.length() == 0) {
            return "/";
        }
        return key.toString();
    }

    // public Map<String, String> getDirKeys() {
    // Map<String, String> keys = new HashMap<String, String>();
    // String fullPath = this.getAbsolutePath();
    // String[] path = fullPath.split("/");
    // int mergeSize = DataServerConfiguration.getDirMergeSize();
    // int i = 1, j = 0;
    // String key = "/";
    // StringBuilder value = new StringBuilder();
    // List<String> k = new ArrayList<String>();
    // List<String> v = new ArrayList<String>();
    // while (i < path.length) {
    // for (j = 0; j < mergeSize && i < path.length; ++j) {
    // value.append("/" + path[i++]);
    // }
    // k.add(key);
    // v.add(value.toString());
    // key = value.toString();
    // }
    // if (j == mergeSize) {
    // k.add(key);
    // v.add(fullPath);
    // }
    // int n = k.size() - 1;
    // if (n >= 0) {
    // keys.put(k.get(n), v.get(n));
    // }
    // if (n > 0) {
    // keys.put(k.get(n - 1), v.get(n - 1));
    // }
    // return keys;
    // }
}
