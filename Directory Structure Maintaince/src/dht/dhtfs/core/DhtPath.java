package dht.dhtfs.core;

import java.io.File;

import dht.dhtfs.core.def.IHashFunction;
import dht.dhtfs.server.datanode.DataRequestProcessor;

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

	public DhtPath getMappingPath() {
		return new DhtPath(fileNameHash(this.getAbsolutePath()));
	}

	public DhtPath getMappingPath(String suffix) {
		return new DhtPath(getMappingPath().getAbsolutePath() + suffix);
	}

	private String fileNameHash(String fileName) {
		int val = hashFun.hashValue(fileName);
		// String hashName = DataRequestProcessor.dataDir + (val & 0xff) + "/" +
		// ((val & 0xff00) >> 8)
		// + "/" + ((val & 0xff0000) >> 16) + "/"
		// + (((val & 0xff000000) >> 24) & 0xff);

		// "/1024/1024/1024/512" for directory
		String hashName = DataRequestProcessor.dataDir + "/" + (val & 0x3ff)
				+ "/" + ((val & 0xffc00) >> 10) + "/"
				+ ((val & 0x3ff00000) >> 20) + "/"
				+ (((val & 0xff800000) >> 23) & 0x1ff);
		return hashName;
	}
}
