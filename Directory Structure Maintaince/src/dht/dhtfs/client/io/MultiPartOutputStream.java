package dht.dhtfs.client.io;

import java.io.IOException;

public abstract class MultiPartOutputStream {
	long size;
	long segmentSize;
	int segmentNum;
	long idx[];
	long byteWritten;

	public MultiPartOutputStream() {

	}

	public MultiPartOutputStream(long size, long segmentSize) {
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
		this.byteWritten = 0;
	}

	public long getByteWritten() {
		return byteWritten;
	}

	public long getOffset(int segmentId) {
		return idx[segmentId] - segmentSize * segmentId;
	}

	public long bytePending(int segmentId) {
		long remain = (segmentId + 1) * segmentSize - idx[segmentId];
		remain = remain > (size - idx[segmentId]) ? (size - idx[segmentId]) : remain;
		return remain;
	}

	public int moveForward(int bufferSize, int segmentId) {
		long remain = bytePending(segmentId);
		if (remain == 0) {
			return -1;
		}
		int len = bufferSize > remain ? (int) remain : bufferSize;
		idx[segmentId] += len;
		return len;
	}

	public int getSegmentNum() {
		return segmentNum;
	}

	abstract public void write(byte[] buf, int len, int segmentId, long blkOffset) throws IOException;

	abstract public void close() throws IOException;
}
