package dht.dhtfs.client.io;

import java.io.IOException;

import dht.dhtfs.server.datanode.FileMeta;

public class MultiPartBufferOutputStreamNew extends MultiPartOutputStreamNew {
	byte[] b;
	final int offset;
	final int len;
	final long filePointer;// file offset

	public MultiPartBufferOutputStreamNew(byte[] b, int offset, int len,
			long filePointer, FileMeta fileMeta) throws IOException {
		if (b == null) {
			throw new NullPointerException("b is null");
		}
		this.b = b;
		this.offset = offset;
		this.len = len;
		this.filePointer = filePointer;
		initialize(len + filePointer, fileMeta);
		long off = blkAbsSizes.get(0).longValue();
		int i;
		for (i = 0; i < noOfSegment && off <= filePointer; ++i, off += blkAbsSizes.get(i).longValue()) {
			idx[i] = blkAbsSizes.get(i + 1);
		}
		idx[i] = filePointer;
	}

	@Override
	public void write(byte[] buf, int len, int segmentId, long blkOffset)
			throws IOException {
		if (buf == null) {
			throw new NullPointerException("buf is null");
		}
		if (buf.length < 1 || segmentId < 0 || segmentId >= noOfSegment
				|| len <= 0) {
			throw new IllegalArgumentException("buf length: " + buf.length
					+ " segmentId: " + segmentId + " (segmentNum: "
					+ noOfSegment + ")");
		}
		if (blkOffset < 0 || blkOffset + segmentId * noOfSegment >= size
				|| blkOffset >= noOfSegment || len > buf.length) {
			throw new ArrayIndexOutOfBoundsException("buf length: "
					+ buf.length + " len: " + len + " offset: " + blkOffset
					+ " size: " + size);
		}
		int i, j;
		for (i = (int) (blkOffset + blkAbsSizes.get(segmentId) - filePointer + offset), j = 0; j < len; ++i, ++j) {
			b[i] = buf[j];
		}
		byteWritten += len;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
