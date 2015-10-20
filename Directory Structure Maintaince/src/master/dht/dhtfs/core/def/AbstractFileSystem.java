package master.dht.dhtfs.core.def;

import java.io.IOException;

/**
 * @author Yinzi Chen
 * @date May 6, 2014
 */
public class AbstractFileSystem implements IFileSystem {

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#initialize(master.dht.dhtfs.core.Configuration)
     */
    @Override
    public void initialize() throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#create(master.dht.dhtfs.core.String)
     */
    @Override
    public IFile create(String path) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#open(master.dht.dhtfs.core.String)
     */
    @Override
    public IFile open(String path) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#open(master.dht.dhtfs.core.String, int)
     */
    @Override
    public IFile open(String path, int mode) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#delete(master.dht.dhtfs.core.String)
     */
    @Override
    public void delete(String path) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#rename(master.dht.dhtfs.core.String,
     * master.dht.dhtfs.core.String)
     */
    @Override
    public void rename(String srcPath, String dstPath) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#copy(master.dht.dhtfs.core.String,
     * master.dht.dhtfs.core.String)
     */
    @Override
    public void copy(String srcPath, String dstPath) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#copyFromLocal(master.dht.dhtfs.core.String,
     * master.dht.dhtfs.core.String)
     */
    @Override
    public void copyFromLocal(String srcPath, String dstPath)
            throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#copyToLocal(master.dht.dhtfs.core.String,
     * master.dht.dhtfs.core.String)
     */
    @Override
    public void copyToLocal(String srcPath, String dstPath) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#mkdir(master.dht.dhtfs.core.String)
     */
    @Override
    public void mkdir(String path) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#rmdir(master.dht.dhtfs.core.String, boolean)
     */
    @Override
    public void rmdir(String path, boolean recursive) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#listStatus(master.dht.dhtfs.core.String)
     */
    @Override
    public void listStatus(String path) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#isDirectory(master.dht.dhtfs.core.String)
     */
    @Override
    public boolean isDirectory(String path) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#isFile(master.dht.dhtfs.core.String)
     */
    @Override
    public boolean isFile(String path) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#exists(master.dht.dhtfs.core.String)
     */
    @Override
    public boolean exists(String path) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

}
