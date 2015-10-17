package dht.dhtfs.client.io;

import java.io.IOException;

public class MultiPartBufferInputStream extends MultiPartInputStream {
	final byte[] b;
	final int offset;
	final int len;
	final long fileOffset;// file offset

	/**
	 * create multi-part input stream from b [offset,offset+len), support
	 * concurrent read for different segments
	 * 
	 * @param b
	 *            bytes to read from
	 * @param offset
	 *            b's read offset
	 * @param len
	 *            b's read len
	 * @param fileOffset
	 *            buffer's global offset in the file
	 * @param segmentSize
	 * @throws IOException
	 */
	public MultiPartBufferInputStream(byte[] b, int offset, int len, long fileOffset, long segmentSize)
			throws IOException {
		if (b == null) {
			throw new NullPointerException("b is null");
		}
		if (offset < 0 || len < 0 || len > b.length - offset) {
			throw new IndexOutOfBoundsException("b size: " + b.length + " offset: " + offset + " len: " + len);
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
	public int read(byte[] buf, int segmentId) {
		if (buf == null) {
			throw new NullPointerException("buf is null");
		}
		if (buf.length < 1 || segmentId < 0 || segmentId >= segmentNum) {
			throw new IllegalArgumentException(
					"buf length: " + buf.length + " segmentId: " + segmentId + " (segmentNum: " + segmentNum + ")");
		}
		long remain = remaining(segmentId);
		if (remain == 0)
			return -1;
		int len = buf.length > remain ? (int) remain : buf.length;
		for (int i = 0; i < len; ++i) {
			buf[i] = b[(int) (idx[segmentId] + i - fileOffset) + offset];
		}
		idx[segmentId] += len;
		byteRead += len;
		return len;
	}

	@Override
	public void close() throws IOException {

	}

}
