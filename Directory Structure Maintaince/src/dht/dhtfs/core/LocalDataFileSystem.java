package dht.dhtfs.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import dht.dhtfs.core.def.AbstractFileSystem;
import dht.dhtfs.core.def.IFile;

/**
 * @author Yinzi Chen
 * @date Mar 26, 2014
 */
public class LocalDataFileSystem extends AbstractFileSystem {

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
    public IFile open(DhtPath path) throws IOException {
        IFile file = LocalDataFile.open(path, IFile.READ);
        return file;
    }

    @Override
    public IFile open(DhtPath path, int mode) throws IOException {
        IFile file = LocalDataFile.open(path, mode);
        return file;
    }

    @Override
    public void copy(DhtPath srcPath, DhtPath dstPath) throws IOException {
        Files.copy(new File(srcPath.getAbsolutePath()).toPath(), new File(
                dstPath.getAbsolutePath()).toPath());
    }

    @Override
    public boolean exists(DhtPath path) throws IOException {
        return new File(path.getAbsolutePath()).exists();
    }

}
