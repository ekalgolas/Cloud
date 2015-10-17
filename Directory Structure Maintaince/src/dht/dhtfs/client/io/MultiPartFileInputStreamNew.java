package dht.dhtfs.client.io;

import java.io.IOException;

import dht.dhtfs.server.datanode.FileMeta;

public class MultiPartFileInputStreamNew extends MultiPartInputStreamNew {
	BufferedRandomAccessFile raf;

	public MultiPartFileInputStreamNew(String fileName, FileMeta fileMeta) throws IOException {
		raf = new BufferedRandomAccessFile(fileName, "r");
		initialize(raf.length(), fileMeta);
	}

	@Override
	public int read(byte[] buf, int segmentId) throws IOException {
		if (buf == null) {
			throw new NullPointerException("buf is null");
		}
		if (buf.length < 1 || segmentId < 0 || segmentId >= noOfSegment) {
			throw new IllegalArgumentException(
					"buf length: " + buf.length + " segmentId: " + segmentId + " (segmentNum: " + noOfSegment + ")");
		}
		long remain = remaining(segmentId);
		if (remain == 0)
			return -1;
		int len = -1;
		synchronized (raf) {
			raf.seek(idx[segmentId]);
			len = raf.read(buf, 0, buf.length > remain ? (int) remain : buf.length);
		}
		idx[segmentId] += len;
		byteRead += len;
		return len;
	}

	@Override
	public void close() throws IOException {
		if (raf != null) {
			raf.close();
		}
	}
}
