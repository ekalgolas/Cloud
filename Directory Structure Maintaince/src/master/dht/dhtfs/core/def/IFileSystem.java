package master.dht.dhtfs.core.def;

import java.io.IOException;

public interface IFileSystem {

    public void initialize() throws IOException;

    // Create a file and open to write, throws IOException if its directory
    // doesn't exist
    public IFile create(String path) throws IOException;

    // Open a file, throws IOException if the file doesn't exist
    public IFile open(String path) throws IOException;

    // Open a file in mode, throws IOException if the file doesn't exist
    public IFile open(String path, int mode) throws IOException;

    // Delete the file
    public void delete(String path) throws IOException;

    // Rename the file or directory, whether replace depending on the
    // implementation
    public void rename(String srcPath, String dstPath) throws IOException;

    public void copy(String srcPath, String dstPath) throws IOException;

    public void copyFromLocal(String srcPath, String dstPath)
            throws IOException;

    public void copyToLocal(String srcPath, String dstPath) throws IOException;

    // Make directory recursively
    public void mkdir(String path) throws IOException;

    // Delete the directory
    public void rmdir(String path, boolean recursive) throws IOException;

    // List status of file or directory
    public void listStatus(String path) throws IOException;

    public boolean isDirectory(String path) throws IOException;

    public boolean isFile(String path) throws IOException;

    // Check whether the file or directory exists
    public boolean exists(String path) throws IOException;

}
