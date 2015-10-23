package master.dht.dhtfs.core;

import java.io.File;
import java.io.IOException;

import master.dht.dhtfs.core.def.AbstractFileSystem;
import master.dht.dhtfs.core.def.IFile;

/**
 * @author Yinzi Chen
 * @date May 4, 2014
 */
public class LocalMetaFileSystem extends AbstractFileSystem {

    @Override
    public void initialize() throws IOException {

    }

    @Override
    public IFile create(String fileName) throws IOException {
        File path = new File(fileName);
        if (new File(fileName).exists()) {
            throw new IOException("create failed, file already exists: "
                    + fileName);
        }
        File dir = new File(path.getParentFile().getAbsolutePath());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        IFile file = LocalMetaFile.create(fileName);
        return file;
    }

    @Override
    public IFile open(String path, int mode) throws IOException {
        IFile file = LocalMetaFile.open(path, mode);
        return file;
    }

    @Override
    public IFile open(String path) throws IOException {
        IFile file = LocalMetaFile.open(path, IFile.READ);
        return file;
    }

    @Override
    public void delete(String fileName) throws IOException {
        File f = new File(fileName);
        if (!f.exists()) {
            throw new IOException("delete failed, file not exists: " + fileName);
        }
        f.renameTo(new File(fileName + ".tombstone"));
    }

}
