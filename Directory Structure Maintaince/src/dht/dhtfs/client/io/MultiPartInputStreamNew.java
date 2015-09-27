package dht.dhtfs.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dht.dhtfs.server.datanode.FileMeta;

public abstract class MultiPartInputStreamNew {
	long size;
	int noOfSegment;
	long idx[];
	long byteRead;
	FileMeta fileMeta;
	List<Long> blkAbsSizes;

	public MultiPartInputStreamNew() {
	}

	public MultiPartInputStreamNew(long size, FileMeta fileMeta) {
		initialize(size, fileMeta);

	}

	public void initialize(long size, FileMeta fileMeta) {
		if (size < 0) {
			throw new IllegalArgumentException("size: " + size);
		}
		if (fileMeta == null) {
			throw new NullPointerException("FileMeta is null");
		}

		this.size = size;
		this.fileMeta = fileMeta;
		this.noOfSegment = fileMeta.getBlkNum();
		this.idx = new long[noOfSegment];
		this.blkAbsSizes = new ArrayList<Long>();
		long index = 0;
		long blkSize = (size / noOfSegment) + 1;
		for (int i = 0; i < noOfSegment; ++i) {
			idx[i] = index;
			blkAbsSizes.add(index);
			index += blkSize;
		}
		this.byteRead = 0;
	}

	public long getByteRead() {
		return byteRead;
	}

	public long getOffset(int segmentId) {
		return idx[segmentId] - blkAbsSizes.get(segmentId);
	}

	public long remaining(int segmentId) {
		long remain;
		if (segmentId == (noOfSegment - 1)) {
			remain = size - idx[segmentId];
		} else {
			remain = blkAbsSizes.get(segmentId + 1) - idx[segmentId];
		}
		return remain;
	}

	public int moveForward(int bufferSize, int segmentId) {
		long remain = remaining(segmentId);
		if (remain == 0) {
			return -1;
		}
		int len = bufferSize > remain ? (int) remain : bufferSize;
		idx[segmentId] += len;
		return len;
	}

	public int getNoOfSegment() {
		return noOfSegment;
	}

	abstract public int read(byte[] buf, int segmentId) throws IOException;

	abstract public void close() throws IOException;
}
