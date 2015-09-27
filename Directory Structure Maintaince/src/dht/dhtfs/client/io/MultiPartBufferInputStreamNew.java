package dht.dhtfs.client.io;

import java.io.IOException;

import dht.dhtfs.server.datanode.FileMeta;

public class MultiPartBufferInputStreamNew extends MultiPartInputStreamNew {
	final byte[] b;
	final int offset;
	final int len;
	final long filePointer;// file offset

	public MultiPartBufferInputStreamNew(byte[] b, int offset, int len,
			long filePointer, FileMeta fileMeta) throws IOException {
		if (b == null) {
			throw new NullPointerException("b is null");
		}
		if (offset < 0 || len < 0 || len > b.length - offset) {
			throw new IndexOutOfBoundsException("b size: " + b.length
					+ " offset: " + offset + " len: " + len);
		}
		this.b = b;
		this.offset = offset;
		this.len = len;
		this.filePointer = filePointer;

		initialize(len + filePointer, fileMeta);

		long off = blkAbsSizes.get(0);
		int i;
		for (i = 0; i < noOfSegment && off <= filePointer; ++i, off += blkAbsSizes
				.get(i)) {
			idx[i] = blkAbsSizes.get(i + 1);
		}
		idx[i] = filePointer;
	}

	@Override
	public int read(byte[] buf, int segmentId) {
		if (buf == null) {
			throw new NullPointerException("buf is null");
		}
		if (buf.length < 1 || segmentId < 0 || segmentId >= noOfSegment) {
			throw new IllegalArgumentException("buf length: " + buf.length
					+ " segmentId: " + segmentId + " (segmentNum: "
					+ noOfSegment + ")");
		}
		long remain = remaining(segmentId);
		if (remain == 0)
			return -1;
		int len = buf.length > remain ? (int) remain : buf.length;
		for (int i = 0; i < len; ++i) {
			buf[i] = b[(int) (idx[segmentId] + i - filePointer) + offset];
		}
		idx[segmentId] += len;
		byteRead += len;
		return len;
	}

	@Override
	public void close() throws IOException {

	}
}
