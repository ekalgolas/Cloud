package dht.dhtfs.core.def;

import java.io.IOException;

public interface IFile {

    static final int READ = 0x00;
    static final int WRITE = 0x01;

    /**
     * Closes this file stream and releases any system resources associated with
     * the stream.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    public void close() throws IOException;

    /**
     * Sets the length of this file.
     * 
     * If the present length of the file as returned by the length method is
     * greater than the newLength argument then the file will be truncated. In
     * this case, if the file offset as returned by the getFilePointer method is
     * greater than newLength then after this method returns the offset will be
     * equal to newLength.
     * 
     * If the present length of the file as returned by the length method is
     * smaller than the newLength argument then the file will be extended. In
     * this case, the contents of the extended portion of the file are not
     * defined.
     * 
     * @param newLength
     *            The desired length of the file
     * @throws IOException
     *             If an I/O error occurs
     */
    public void setLength(long newLength) throws IOException;

    /**
     * @return Returns the current offset in this file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public long getFilePointer() throws IOException;

    /**
     * @return the length of this file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public long length() throws IOException;

    /**
     * Reads up to b.length bytes of data from this file into an array of bytes.
     * 
     * @param b
     *            the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of this file has been reached.
     * @throws IOException
     *             If the first byte cannot be read for any reason other than
     *             end of file, or if the random access file has been closed, or
     *             if some other I/O error occurs.
     * 
     * @throws NullPointerException
     *             If b is null.
     */
    public int read(byte[] b) throws IOException;

    /**
     * Reads up to len bytes of data from this file into an array of bytes.
     * 
     * @param b
     *            the buffer into which the data is read.
     * @param off
     *            the start offset in array b at which the data is written.
     * @param len
     *            the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of the file has been reached.
     * @throws IOException
     *             If the first byte cannot be read for any reason other than
     *             end of file, or if the file has been closed, or if some other
     *             I/O error occurs.
     * @throws NullPointerException
     *             If b is null.
     * @throws IndexOutOfBoundsException
     *             If off is negative, len is negative, or len is greater than
     *             b.length - off
     */
    public int read(byte[] b, int off, int len) throws IOException;

    /**
     * Writes b.length bytes from the specified byte array to this file,
     * starting at the current file pointer.
     * 
     * @param b
     *            the data to be written.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public void write(byte[] b) throws IOException;

    /**
     * Writes len bytes from the specified byte array starting at offset off to
     * this file.
     * 
     * @param b
     *            the data to be written.
     * @param off
     *            the start offset in the data.
     * @param len
     *            the number of bytes to write.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public void write(byte[] b, int off, int len) throws IOException;

    public void insert(byte[] b) throws IOException;

    public void insert(byte[] b, int off, int len) throws IOException;

    /**
     * Sets the file-pointer offset, measured from the beginning of this file,
     * at which the next read or write occurs. The offset should not be set
     * beyond the end of the file.
     * 
     * @param pos
     *            the offset position, measured in bytes from the beginning of
     *            the file, at which to set the file pointer.
     * @throws IOException
     *             if pos is less than 0 or if an I/O error occurs.
     */
    public void seek(long pos) throws IOException;

    public void flush() throws IOException;

    public String checkSum() throws IOException;

}
