package dht.dhtfs.client.io;

import java.io.IOException;

public class MultiPartBufferOutputStream extends MultiPartOutputStream {
	byte[] b;
	final int offset;
	final int len;
	final long fileOffset;// file offset

	/**
	 * create multi-part output stream to b [offset,offset+len), support
	 * concurrent write for different segments
	 * 
	 * @param b
	 *            bytes to write to
	 * @param offset
	 *            b's write offset
	 * @param len
	 *            b's write len
	 * @param fileOffset
	 *            buffer's global offset in the file
	 * @param segmentSize
	 * @throws IOException
	 */
	public MultiPartBufferOutputStream(byte[] b, int offset, int len, long fileOffset, long segmentSize)
			throws IOException {
		if (b == null) {
			throw new NullPointerException("b is null");
		}
		this.b = b;
		this.offset = offset;
		this.len = len;
		this.fileOffset = fileOffset;
		initialize(len + fileOffset, segmentSize);
		long off = segmentSize;
		int i;
		for (i = 0; i < segmentNum && off <= fileOffset; ++i, off += segmentSize) {
			idx[i] = segmentSize * (i + 1);
		}
		idx[i] = fileOffset;
	}

	@Override
	public void write(byte[] buf, int len, int segmentId, long blkOffset) throws IOException {
		if (buf == null) {
			throw new NullPointerException("buf is null");
		}
		if (buf.length < 1 || segmentId < 0 || segmentId >= segmentNum || len <= 0) {
			throw new IllegalArgumentException(
					"buf length: " + buf.length + " segmentId: " + segmentId + " (segmentNum: " + segmentNum + ")");
		}
		if (blkOffset < 0 || blkOffset + segmentId * segmentSize >= size || blkOffset >= segmentSize
				|| len > buf.length) {
			throw new ArrayIndexOutOfBoundsException(
					"buf length: " + buf.length + " len: " + len + " offset: " + blkOffset + " size: " + size);
		}
		int i, j;
		for (i = (int) (blkOffset + segmentId * segmentSize - fileOffset + offset), j = 0; j < len; ++i, ++j) {
			b[i] = buf[j];
		}
		byteWritten += len;
	}

	@Override
	public void close() throws IOException {

	}

}
