package dht.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.base.Preconditions;

import dht.hdfs.server.protocol.DatanodeID;

public class INodeFile extends INode {
	public INodeFile(INode parentNode) {
		super(parentNode);
	}

	public INodeFile(long id, byte[] name) {
		super(null, id, name);
	}

	INodeFile(long id, byte[] name, BlockInfo[] blklist, short replication,
			long preferredBlockSize) {
		super(null, id, name);
		header = HeaderFormat.combineReplication(header, replication);
		header = HeaderFormat.combinePreferredBlockSize(header,
				preferredBlockSize);
		this.blocks = blklist;
	}

	public static INodeFile valueOf(INode inode, String path)
			throws FileNotFoundException {
		return valueOf(inode, path, false);
	}

	public static INodeFile valueOf(INode inode, String path, boolean acceptNull)
			throws FileNotFoundException {
		if (inode == null) {
			if (acceptNull) {
				return null;
			} else {
				throw new FileNotFoundException("File does not exist: " + path);
			}
		}
		if (!inode.isFile()) {
			throw new FileNotFoundException("Path is not a file: " + path);
		}
		return inode.asFile();
	}

	/** Format: [16 bits for replication][48 bits for PreferredBlockSize] */
	static class HeaderFormat {
		/** Number of bits for Block size */
		static final int BLOCKBITS = 48;
		/** Header mask 64-bit representation */
		static final long HEADERMASK = 0xffffL << BLOCKBITS;
		static final long MAX_BLOCK_SIZE = ~HEADERMASK;

		static short getReplication(long header) {
			return (short) ((header & HEADERMASK) >> BLOCKBITS);
		}

		static long combineReplication(long header, short replication) {
			if (replication <= 0) {
				throw new IllegalArgumentException(
						"Unexpected value for the replication: " + replication);
			}
			return ((long) replication << BLOCKBITS)
					| (header & MAX_BLOCK_SIZE);
		}

		static long getPreferredBlockSize(long header) {
			return header & MAX_BLOCK_SIZE;
		}

		static long combinePreferredBlockSize(long header, long blockSize) {
			if (blockSize < 0) {
				throw new IllegalArgumentException("Block size < 0: "
						+ blockSize);
			} else if (blockSize > MAX_BLOCK_SIZE) {
				throw new IllegalArgumentException("Block size = " + blockSize
						+ " > MAX_BLOCK_SIZE = " + MAX_BLOCK_SIZE);
			}
			return (header & HEADERMASK) | (blockSize & MAX_BLOCK_SIZE);
		}
	}

	@Override
	public final boolean isFile() {
		return true;
	}

	@Override
	public final INodeFile asFile() {
		return this;
	}

	public boolean isUnderConstruction() {
		return false;
	}

	private long header = 0L;
	private BlockInfo[] blocks;

	public INodeFile(INodeFile that) {
		super(that);
		this.header = that.header;
	}

	public final short getFileReplication() {
		return HeaderFormat.getReplication(header);
	}

	public final void setFileReplication(short replication) {
		header = HeaderFormat.combineReplication(header, replication);
	}

	public long getPreferredBlockSize() {
		return HeaderFormat.getPreferredBlockSize(header);
	}

	public long getHeaderLong() {
		return header;
	}

	public String getName() {
		return getFullPathName();
	}

	public BlockInfo[] getBlocks() {
		return this.blocks;
	}

	public void setBlocks(BlockInfo[] blocks) {
		this.blocks = blocks;
	}

	public int numBlocks() {
		return blocks == null ? 0 : blocks.length;
	}

	public void setBlock(int idx, BlockInfo blk) {
		this.blocks[idx] = blk;
	}

	public BlockInfo getLastBlock() throws IOException {
		return blocks == null || blocks.length == 0 ? null
				: blocks[blocks.length - 1];
	}

	boolean removeLastBlock(Block oldblock) throws IOException {
		if (blocks == null || blocks.length == 0) {
			return false;
		}
		int size_1 = blocks.length - 1;
		if (!blocks[size_1].equals(oldblock)) {
			return false;
		}

		// copy to a new list
		BlockInfo[] newlist = new BlockInfo[size_1];
		System.arraycopy(blocks, 0, newlist, 0, size_1);
		setBlocks(newlist);
		return true;
	}

	public short getBlockReplication() {
		return HeaderFormat.getReplication(header);
	}

	/**
	 * add a block to the block list
	 */
	void addBlock(BlockInfo newblock) {
		if (this.blocks == null) {
			this.setBlocks(new BlockInfo[] { newblock });
		} else {
			int size = this.blocks.length;
			BlockInfo[] newlist = new BlockInfo[size + 1];
			System.arraycopy(this.blocks, 0, newlist, 0, size);
			newlist[size] = newblock;
			this.setBlocks(newlist);
		}
	}

	public INodeFileUnderConstruction toUnderConstruction(String clientName,
			String clientMachine, DatanodeID clientNode) {
		Preconditions.checkState(!isUnderConstruction(),
				"file is already an INodeFileUnderConstruction");
		return new INodeFileUnderConstruction(this, clientName, clientMachine,
				clientNode);
	}
}
