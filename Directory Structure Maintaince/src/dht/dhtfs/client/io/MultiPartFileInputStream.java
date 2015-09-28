package dht.dhtfs.client.io;

import java.io.IOException;

public class MultiPartFileInputStream extends MultiPartInputStream {
	// Need improve to buffered RandomAccessFile
	// TODO
	BufferedRandomAccessFile raf;

	public MultiPartFileInputStream(String fileName, long segmentSize)
			throws IOException {
		raf = new BufferedRandomAccessFile(fileName, "r");
		System.out.println("filelen: " + raf.length());
		initialize(raf.length(), segmentSize);
	}

	@Override
	public int read(byte[] buf, int segmentId) throws IOException {
		if (buf == null) {
			throw new NullPointerException("buf is null");
		}
		if (buf.length < 1 || segmentId < 0 || segmentId >= segmentNum) {
			throw new IllegalArgumentException("buf length: " + buf.length
					+ " segmentId: " + segmentId + " (segmentNum: "
					+ segmentNum + ")");
		}
		long remain = remaining(segmentId);
		if (remain == 0)
			return -1;
		int len = -1;
		synchronized (raf) {
			raf.seek(idx[segmentId]);
			len = raf.read(buf, 0, buf.length > remain ? (int) remain
					: buf.length);
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
