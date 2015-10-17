package dht.dhtfs.client.io;

import java.io.IOException;

import dht.dhtfs.server.datanode.FileMeta;

public class MultiPartFileOutputStreamNew extends MultiPartOutputStreamNew {

	BufferedRandomAccessFile raf;

	public MultiPartFileOutputStreamNew(String fileName, long fileSize, FileMeta fileMeta) throws IOException {
		raf = new BufferedRandomAccessFile(fileName, "rw");
		raf.setLength(fileSize);
		initialize(fileSize, fileMeta);
	}

	@Override
	public void write(byte[] buf, int len, int segmentId, long blkOffset) throws IOException {
		if (buf == null) {
			throw new NullPointerException("buf is null");
		}
		if (buf.length < 1 || segmentId < 0 || segmentId >= noOfSegment || len <= 0) {
			throw new IllegalArgumentException(
					"buf length: " + buf.length + " segmentId: " + segmentId + " (segmentNum: " + noOfSegment + ")");
		}
		if (blkOffset < 0 || blkOffset + segmentId * noOfSegment >= size || blkOffset >= noOfSegment
				|| len > buf.length) {
			throw new ArrayIndexOutOfBoundsException(
					"buf length: " + buf.length + " len: " + len + " offset: " + blkOffset + " size: " + size);
		}
		synchronized (raf) {
			raf.seek(blkOffset + blkAbsSizes.get(segmentId));
			raf.write(buf, 0, len);
		}
		byteWritten += len;
	}

	@Override
	public void close() throws IOException {
		if (raf != null) {
			raf.close();
		}
	}

}
