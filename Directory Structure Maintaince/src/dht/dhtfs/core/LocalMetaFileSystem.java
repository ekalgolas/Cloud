package dht.dhtfs.core;

import java.io.File;
import java.io.IOException;

import dht.dhtfs.core.def.AbstractFileSystem;
import dht.dhtfs.core.def.IFile;

/**
 * @author Yinzi Chen
 * @date May 4, 2014
 */
public class LocalMetaFileSystem extends AbstractFileSystem {

	@Override
	public void initialize(Configuration conf) throws IOException {

	}

	@Override
	public IFile create(DhtPath path) throws IOException {
		String fileName = path.getAbsolutePath();
		if (new File(fileName).exists()) {
			throw new IOException("create failed, file already exists: "
					+ fileName);
		}
		File dir = new File(path.getParentFile().getAbsolutePath());
		if (!dir.exists()) {
			dir.mkdirs();
		}
		IFile file = LocalMetaFile.create(path);
		return file;
	}

	@Override
	public IFile open(DhtPath path, int mode) throws IOException {
		IFile file = LocalMetaFile.open(path, mode);
		return file;
	}

	@Override
	public IFile open(DhtPath path) throws IOException {
		IFile file = LocalMetaFile.open(path, IFile.READ);
		return file;
	}

	@Override
	public void delete(DhtPath path) throws IOException {
		String fileName = path.getAbsolutePath();
		File f = new File(fileName);
		if (!f.exists()) {
			throw new IOException("delete failed, file not exists: " + fileName);
		}
		f.renameTo(new File(fileName + ".tombstone"));
	}

}
