package master.dht.dhtfs.core.def;

import java.io.IOException;

public interface IDFSFile extends IFile {

    /**
     * Commit, which makes the modification visible to other process
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    public void commit() throws IOException;

    public void download(String dest) throws IOException;

    public void upload(String src) throws IOException;

    public void lock(long pos, long len) throws IOException;
}
