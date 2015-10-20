package master.dht.dhtfs.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import master.dht.dhtfs.core.def.AbstractFileSystem;
import master.dht.dhtfs.core.def.IFile;

/**
 * @author Yinzi Chen
 * @date Mar 26, 2014
 */
public class LocalBlockFileSystem extends AbstractFileSystem {

    @Override
    public void initialize() throws IOException {

    }

    @Override
    public IFile create(String fileName) throws IOException {
        if (new File(fileName).exists()) {
            throw new IOException("create failed, file already exists: "
                    + fileName);
        }
        File dir = new File(new File(fileName).getParentFile()
                .getAbsolutePath());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        IFile file = LocalMetaFile.create(fileName);
        return file;
    }

    @Override
    public IFile open(String path) throws IOException {
        IFile file = LocalBlockFile.open(path, IFile.READ);
        return file;
    }

    @Override
    public IFile open(String path, int mode) throws IOException {
        IFile file = LocalBlockFile.open(path, mode);
        return file;
    }

    @Override
    public void copy(String srcPath, String dstPath) throws IOException {
        Files.copy(new File(srcPath).toPath(), new File(dstPath).toPath());
    }

}