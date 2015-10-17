package dht.dhtfs.client.io;

import java.io.IOException;

public class MultiPartFileOutputStream extends MultiPartOutputStream {
	// Need improve to buffered RandomAccessFile
	// TODO
	BufferedRandomAccessFile raf;

	public MultiPartFileOutputStream(String fileName, long fileSize, long segmentSize) throws IOException {
		raf = new BufferedRandomAccessFile(fileName, "rw");
		raf.setLength(fileSize);
		initialize(fileSize, segmentSize);
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
		synchronized (raf) {
			raf.seek(blkOffset + segmentId * segmentSize);
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
