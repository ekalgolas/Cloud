package dht.dhtfs.client.io;

import java.io.IOException;

public abstract class MultiPartInputStream {
	long size;
	long segmentSize;
	int segmentNum;
	long idx[];
	long byteRead;

	public MultiPartInputStream() {

	}

	public MultiPartInputStream(long size, long segmentSize) {
		initialize(size, segmentSize);
	}

	public void initialize(long size, long segmentSize) {
		if (size < 0 || segmentSize <= 0) {
			throw new IllegalArgumentException("size: " + size + " segmentSize: " + segmentSize);
		}
		this.size = size;
		this.segmentSize = segmentSize;
		this.segmentNum = (int) ((size - 1 + segmentSize) / segmentSize);
		this.idx = new long[segmentNum];
		for (int i = 0; i < segmentNum; ++i) {
			idx[i] = segmentSize * i;
		}
		this.byteRead = 0;
	}

	public long getByteRead() {
		return byteRead;
	}

	/**
	 * 
	 * @param segmentId
	 * @return offset in the segment
	 */
	public long getSegOffset(int segmentId) {
		return idx[segmentId] - segmentSize * segmentId;
	}

	public int getSegmentNum() {
		return segmentNum;
	}

	abstract public int read(byte[] buf, int segmentId) throws IOException;

	abstract public void close() throws IOException;

	public long remaining(int segmentId) {
		long remain = (segmentId + 1) * segmentSize - idx[segmentId];
		remain = remain > (size - idx[segmentId]) ? (size - idx[segmentId]) : remain;
		return remain;
	}
}
