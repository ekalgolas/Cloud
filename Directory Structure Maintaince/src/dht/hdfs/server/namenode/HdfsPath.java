package dht.hdfs.server.namenode;

import java.io.File;

public class HdfsPath extends File {
	private static final long serialVersionUID = 1L;

	public HdfsPath(String path) {
		super(path);

		// Comment for Windows OS
		if (path.length() < 1 /* || path.charAt(0) != File.separatorChar */) {
			throw new IllegalArgumentException("path should be an absolute path, path: " + path);
		}
	}

	public HdfsPath getDir() {
		return new HdfsPath(super.getParent());
	}

	public String toString() {
		return this.getAbsolutePath();
	}
}
