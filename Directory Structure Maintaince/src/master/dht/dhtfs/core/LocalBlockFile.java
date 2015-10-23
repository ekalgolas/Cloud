package master.dht.dhtfs.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import master.dht.dhtfs.core.def.IFile;

/**
 * @author Yinzi Chen
 * @date Mar 27, 2014
 */
public class LocalBlockFile implements IFile {
    private String fileName;
    private RandomAccessFile file;

    protected LocalBlockFile(String path, int mode) throws IOException {
        String m = "r";
        if ((mode & WRITE) == WRITE) {
            m = "rw";
        }
        this.fileName = path;
        this.file = new RandomAccessFile(fileName, m);
    }

    public static LocalBlockFile create(String path) throws IOException {
        return new LocalBlockFile(path, READ | WRITE);
    }

    public static LocalBlockFile open(String path, int mode) throws IOException {
        return new LocalBlockFile(path, mode);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#close()
     */
    @Override
    public void close() throws IOException {
        file.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#setLength(long)
     */
    @Override
    public void setLength(long newLength) throws IOException {
        file.setLength(newLength);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#getFilePointer()
     */
    @Override
    public long getFilePointer() throws IOException {
        return file.getFilePointer();
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#length()
     */
    @Override
    public long length() throws IOException {
        return file.length();
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#read(byte[])
     */
    @Override
    public int read(byte[] b) throws IOException {
        return file.read(b);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#read(byte[], int, int)
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return file.read(b, off, len);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#write(byte[])
     */
    @Override
    public void write(byte[] b) throws IOException {
        file.write(b);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#write(byte[], int, int)
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        file.write(b, off, len);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#seek(long)
     */
    @Override
    public void seek(long pos) throws IOException {
        file.seek(pos);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#flush()
     */
    @Override
    public void flush() throws IOException {

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#insert(byte[])
     */
    @Override
    public void insert(byte[] b) throws IOException {
        insert(b, 0, b.length);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#insert(byte[], int, int)
     */
    @Override
    public void insert(byte[] b, int off, int len) throws IOException {
        MappedByteBuffer buf = file.getChannel().map(
                FileChannel.MapMode.READ_WRITE, 0, file.length() + len);
        for (int i = buf.capacity() - 1; i >= file.getFilePointer() + len; --i) {
            buf.put(i, buf.get(i - len));
        }
        for (int i = 0; i < len; ++i) {
            buf.put(i + (int) file.getFilePointer(), b[i]);
        }
        file.seek(file.getFilePointer() + len);
        buf.force();
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFile#checkSum()
     */
    @Override
    public String checkSum() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
