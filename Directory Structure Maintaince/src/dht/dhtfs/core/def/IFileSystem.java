package dht.dhtfs.core.def;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;

public interface IFileSystem {

    public void initialize(Configuration conf) throws IOException;

    // Create a file and open to write, throws IOException if its directory
    // doesn't exist
    public IFile create(DhtPath path) throws IOException;

    // Open a file, throws IOException if the file doesn't exist
    public IFile open(DhtPath path) throws IOException;

    // Open a file in mode, throws IOException if the file doesn't exist
    public IFile open(DhtPath path, int mode) throws IOException;

    // Delete the file
    public void delete(DhtPath path) throws IOException;

    // Rename the file or directory, whether replace depending on the
    // implementation
    public void rename(DhtPath srcPath, DhtPath dstPath) throws IOException;

    public void copy(DhtPath srcPath, DhtPath dstPath) throws IOException;

    public void copyFromLocal(DhtPath srcPath, DhtPath dstPath)
            throws IOException;

    public void copyToLocal(DhtPath srcPath, DhtPath dstPath)
            throws IOException;

    // Make directory recursively
    public void mkdir(DhtPath path) throws IOException;

    // Delete the directory
    public void rmdir(DhtPath path, boolean recursive) throws IOException;

    // List status of file or directory
    public void listStatus(DhtPath path) throws IOException;

    public boolean isDirectory(DhtPath path) throws IOException;

    public boolean isFile(DhtPath path) throws IOException;

    // Check whether the file or directory exists
    public boolean exists(DhtPath path) throws IOException;

}
