package dht.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import dht.hdfs.server.protocol.DatanodeID;

public class FSNamesystem {
	FSDirectory dir;
	private final SequentialBlockIdGenerator blockIdGenerator;
	private ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);
	private INodeId inodeId;

	// private final BlockManager blockManager;

	public static FSNamesystem loadFromDisk() throws IOException {
		FSNamesystem namesystem = new FSNamesystem();
		return namesystem;
	}

	private FSNamesystem() {
		this.blockIdGenerator = new SequentialBlockIdGenerator();
		this.inodeId = new INodeId();
		this.dir = new FSDirectory(this);
	}

	long getLastAllocatedBlockId() {
		return blockIdGenerator.getCurrentValue();
	}

	private long nextBlockId() throws Exception {
		assert hasWriteLock();
		final long blockId = blockIdGenerator.nextValue();
		return blockId;
	}

	/**
	 * Set the last allocated inode id when fsimage or editlog is loaded.
	 */
	public void resetLastInodeId(long newValue) throws IOException {
		try {
			inodeId.skipTo(newValue);
		} catch (IllegalStateException ise) {
			throw new IOException(ise);
		}
	}

	/** @return the last inode ID. */
	public long getLastInodeId() {
		return inodeId.getCurrentValue();
	}

	/** Allocate a new inode ID. */
	public long allocateNewInodeId() {
		return inodeId.nextValue();
	}

	public void readLock() {
		this.fsLock.readLock().lock();
	}

	public void readUnlock() {
		this.fsLock.readLock().unlock();
	}

	public void writeLock() {
		this.fsLock.writeLock().lock();
	}

	public void writeLockInterruptibly() throws InterruptedException {
		this.fsLock.writeLock().lockInterruptibly();
	}

	public void writeUnlock() {
		this.fsLock.writeLock().unlock();
	}

	public boolean hasWriteLock() {
		return this.fsLock.isWriteLockedByCurrentThread();
	}

	public boolean hasReadLock() {
		return this.fsLock.getReadHoldCount() > 0;
	}

	public boolean hasReadOrWriteLock() {
		return hasReadLock() || hasWriteLock();
	}

	/**
	 * Get block locations within the specified range.
	 * 
	 * @see ClientProtocol#getBlockLocations(String, long, long)
	 */
	LocatedBlocks getBlockLocations(String clientMachine, String src,
			long offset, long length) throws Exception, IOException {
		LocatedBlocks blocks = getBlockLocations(src, offset, length, false,
				false, false);
		// if (blocks != null) {
		// blockManager.getDatanodeManager().sortLocatedBlocks(clientMachine,
		// blocks.getLocatedBlocks());
		//
		// LocatedBlock lastBlock = blocks.getLastLocatedBlock();
		// if (lastBlock != null) {
		// ArrayList<LocatedBlock> lastBlockList = new
		// ArrayList<LocatedBlock>();
		// lastBlockList.add(lastBlock);
		// blockManager.getDatanodeManager().sortLocatedBlocks(
		// clientMachine, lastBlockList);
		// }
		// }
		return blocks;
	}

	/**
	 * Get block locations within the specified range.
	 * 
	 * @see ClientProtocol#getBlockLocations(String, long, long)
	 * @throws FileNotFoundException
	 *             , UnresolvedLinkException, IOException
	 */
	LocatedBlocks getBlockLocations(String src, long offset, long length,
			boolean doAccessTime, boolean needBlockToken, boolean checkSafeMode)
			throws FileNotFoundException, Exception, IOException {
		try {
			return getBlockLocationsInt(src, offset, length, doAccessTime,
					needBlockToken, checkSafeMode);
		} catch (Exception e) {
			// logAuditEvent(false, "open", src);
			throw e;
		}
	}

	private LocatedBlocks getBlockLocationsInt(String src, long offset,
			long length, boolean doAccessTime, boolean needBlockToken,
			boolean checkSafeMode) throws FileNotFoundException, Exception,
			IOException {
		if (offset < 0) {
			throw new IllegalArgumentException(
					"Negative offset is not supported. File: " + src);
		}
		if (length < 0) {
			throw new IllegalArgumentException(
					"Negative length is not supported. File: " + src);
		}
		final LocatedBlocks ret = null;// getBlockLocationsUpdateTimes(src,
										// offset,
		// length, doAccessTime, needBlockToken);
		// logAuditEvent(true, "open", src);

		return ret;
	}

	/*
	 * Get block locations within the specified range, updating the access times
	 * if necessary.
	 */
	public long getBlockLocationsUpdateTimes(String src, long offset,
			long length, boolean doAccessTime, boolean needBlockToken)
			throws FileNotFoundException, Exception, IOException {
		byte[][] pathComponents = null;
		readLock();
		try {

			final INodesInPath iip = dir.getLastINodeInPath(src);
			final INodeFile inode = INodeFile.valueOf(iip.getLastINode(), src);
			// System.out.println("Located file id: " + inode.getId() + " "
			// + inode.getName());
			return System.currentTimeMillis();
		} finally {
			readUnlock();
		}
	}

	public LocatedBlocks createLocatedBlocks(final BlockInfo[] blocks,
			final long offset, final long length, final boolean needBlockToken)
			throws IOException {
		if (blocks == null) {
			return null;
		} else if (blocks.length == 0) {
			return new LocatedBlocks(0, false,
					Collections.<LocatedBlock> emptyList(), null, false);
		} else {
			final List<LocatedBlock> locatedblocks = createLocatedBlockList(
					blocks, offset, length, Integer.MAX_VALUE);

			final LocatedBlock lastlb;
			final boolean isComplete;
			lastlb = createLocatedBlock(blocks, 0l);
			isComplete = true;
			return new LocatedBlocks(0l, false, locatedblocks, lastlb,
					isComplete);
		}
	}

	private List<LocatedBlock> createLocatedBlockList(final BlockInfo[] blocks,
			final long offset, final long length, final int nrBlocksToReturn)
			throws IOException {
		int curBlk = 0;
		long curPos = 0, blkSize = 0;
		int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
		for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
			blkSize = blocks[curBlk].getNumBytes();
			assert blkSize > 0 : "Block of size 0";
			if (curPos + blkSize > offset) {
				break;
			}
			curPos += blkSize;
		}

		if (nrBlocks > 0 && curBlk == nrBlocks) // offset >= end of file
			return Collections.<LocatedBlock> emptyList();

		long endOff = offset + length;
		List<LocatedBlock> results = new ArrayList<LocatedBlock>(blocks.length);
		do {
			results.add(createLocatedBlock(blocks[curBlk], curPos));
			curPos += blocks[curBlk].getNumBytes();
			curBlk++;
		} while (curPos < endOff && curBlk < blocks.length
				&& results.size() < nrBlocksToReturn);
		return results;
	}

	private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos)
			throws IOException {
		final LocatedBlock lb = createLocatedBlock(blk, pos);

		return lb;
	}

	private LocatedBlock createLocatedBlock(final BlockInfo[] blocks,
			final long endPos) throws IOException {
		int curBlk = 0;
		long curPos = 0;
		int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
		for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
			long blkSize = blocks[curBlk].getNumBytes();
			if (curPos + blkSize >= endPos) {
				break;
			}
			curPos += blkSize;
		}
		return createLocatedBlock(blocks[curBlk], curPos);
	}

	long getPreferredBlockSize(String filename) throws IOException, Exception {

		readLock();
		try {
			return dir.getPreferredBlockSize(filename);
		} finally {
			readUnlock();
		}
	}

	private void verifyParentDir(String src) throws FileNotFoundException,
			Exception {
		assert hasReadOrWriteLock();
		HdfsPath parent = new HdfsPath(src).getDir();
		if (parent != null) {
			final INode parentNode = dir.getINode(parent.toString());
			if (parentNode == null) {
				throw new FileNotFoundException(
						"Parent directory doesn't exist: " + parent);
			} else if (!parentNode.isDirectory()) {
				throw new Exception("Parent path is not a directory: " + parent);
			}
		}
	}

	/**
	 * Create a new file entry in the namespace.
	 * 
	 * For description of parameters and exceptions thrown see
	 * {@link ClientProtocol#create()}, except it returns valid file status upon
	 * success
	 * 
	 * For retryCache handling details see -
	 * {@link #getFileStatus(boolean, CacheEntryWithPayload)}
	 * 
	 */
	HdfsFileStatus startFile(String src, String holder, String clientMachine,
			boolean createParent, short replication, long blockSize)
			throws Exception, FileNotFoundException, IOException {
		HdfsFileStatus status = null;

		try {
			status = startFileInt(src, holder, clientMachine, createParent,
					replication, blockSize);
		} catch (Exception e) {
			// logAuditEvent(false, "create", src);
			throw e;
		}
		return status;
	}

	private HdfsFileStatus startFileInt(String src, String holder,
			String clientMachine, boolean createParent, short replication,
			long blockSize) throws Exception, FileNotFoundException,
			IOException {

		boolean skipSync = false;
		HdfsFileStatus stat = null;
		byte[][] pathComponents = null;
		boolean create = true;
		boolean overwrite = false;
		writeLock();
		try {
			startFileInternal(src, holder, clientMachine, create, overwrite,
					createParent, replication, blockSize);
			stat = dir.getFileInfo(src, false);
		} catch (Exception se) {
			skipSync = true;
			throw se;
		} finally {
			writeUnlock();
		}
		return stat;
	}

	/**
	 * Create a new file or overwrite an existing file<br>
	 * 
	 * Once the file is create the client then allocates a new block with the
	 * next call using {@link NameNode#addBlock()}.
	 * <p>
	 * For description of parameters and exceptions thrown see
	 * {@link ClientProtocol#create()}
	 */
	private void startFileInternal(String src, String holder,
			String clientMachine, boolean create, boolean overwrite,
			boolean createParent, short replication, long blockSize)
			throws Exception, FileNotFoundException, IOException {
		assert hasWriteLock();
		// Verify that the destination does not exist as a directory already.
		final INodesInPath iip = dir.getINodesInPath4Write(src);

		final INode inode = iip.getLastINode();
		if (inode != null && inode.isDirectory()) {
			throw new Exception("Cannot create file " + src
					+ "; already exists as a directory.");
		}
		final INodeFile myFile = INodeFile.valueOf(inode, src, true);

		if (!createParent) {
			verifyParentDir(src);
		}

		try {
			if (myFile == null) {
				if (!create) {
					throw new FileNotFoundException(
							"failed to overwrite non-existent file " + src
									+ " on client " + clientMachine);
				}
			} else {
				if (overwrite) {
					try {
					} catch (Exception e) {
						e.printStackTrace();
						throw e;
					}
				} else {
					throw new Exception("failed to create file " + src
							+ " on client " + clientMachine
							+ " because the file exists");
				}
			}

			// System.out.println("Add file src: " + src);
			INodeFile newNode = dir.addFile(src, replication, blockSize,
					holder, clientMachine);

			// dir.dump();
			if (newNode == null) {
				throw new IOException("DIR* NameSystem.startFile: "
						+ "Unable to add file to namespace.");
			}

		} catch (IOException ie) {
			ie.printStackTrace();
			throw ie;
		}
	}

	/**
	 * Create all the necessary directories
	 */
	boolean mkdirs(String src, boolean createParent) throws IOException,
			Exception {
		boolean ret = false;
		try {
			ret = mkdirsInt(src, createParent);
		} catch (Exception e) {
			throw e;
		}
		return ret;
	}

	private boolean mkdirsInt(String src, boolean createParent)
			throws IOException, Exception {

		byte[][] pathComponents = null;
		HdfsFileStatus resultingStat = null;
		boolean status = false;
		writeLock();
		try {
			status = mkdirsInternal(src, createParent);
			if (status) {
				resultingStat = dir.getFileInfo(src, false);
			}
		} finally {
			writeUnlock();
		}
		if (status) {
			// logAuditEvent(true, "mkdirs", src, null, resultingStat);
		}
		return status;
	}

	/**
	 * Create all the necessary directories
	 */
	private boolean mkdirsInternal(String src, boolean createParent)
			throws IOException, Exception {
		assert hasWriteLock();

		if (!createParent) {
			verifyParentDir(src);
		}

		if (!dir.mkdirs(src)) {
			throw new IOException("Failed to create directory: " + src);
		}
		return true;
	}

	// New Added
	LocatedBlock appendFile(String src, String holder, String clientMachine)
			throws IOException {
		LocatedBlock lb = null;

		boolean success = false;
		try {
			lb = appendFileInt(src, holder, clientMachine);
			success = true;
			return lb;
		} catch (Exception e) {
			throw e;
		}
	}

	private LocatedBlock appendFileInt(String src, String holder,
			String clientMachine) throws IOException {

		LocatedBlock lb = null;
		writeLock();
		try {
			lb = appendFileInternal(src, holder, clientMachine);
		} catch (Exception se) {
			se.printStackTrace();
			;
		} finally {
			writeUnlock();
			// There might be transactions logged while trying to recover the
			// lease.
			// They need to be sync'ed even when an exception was thrown.
		}
		return lb;
	}

	private LocatedBlock appendFileInternal(String src, String holder,
			String clientMachine) throws Exception {
		assert hasWriteLock();
		// Verify that the destination does not exist as a directory already.
		final INodesInPath iip = dir.getINodesInPath4Write(src);
		final INode inode = iip.getLastINode();
		if (inode != null && inode.isDirectory()) {
			throw new Exception("Cannot append to directory " + src
					+ "; already exists as a directory.");
		}

		try {
			if (inode == null) {
				throw new FileNotFoundException(
						"failed to append to non-existent file " + src
								+ " for client " + clientMachine);
			}

			INodeFile myFile = INodeFile.valueOf(inode, src, true);
			// Opening an existing file for write - may need to recover lease.
			// recoverLeaseInternal(myFile, src, holder, clientMachine, false);

			// recoverLeaseInternal may create a new InodeFile via
			// finalizeINodeFileUnderConstruction so we need to refresh
			// the referenced file.
			myFile = INodeFile.valueOf(dir.getINode(src), src, true);

			// final DatanodeDescriptor clientNode = blockManager
			// .getDatanodeManager().getDatanodeByHost(clientMachine);
			return prepareFileForWrite(src, myFile, holder, clientMachine, null);
		} catch (IOException ie) {
			throw ie;
		}
	}

	LocatedBlock prepareFileForWrite(String src, INodeFile file,
			String leaseHolder, String clientMachine, DatanodeID clientNode)
			throws IOException {
		final INodeFile cons = file.toUnderConstruction(leaseHolder,
				clientMachine, clientNode);

		// LocatedBlock ret = blockManager
		// .convertLastBlockToUnderConstruction(cons);

		// return ret;
		return null;
	}

	/**
	 * Save allocated block at the given pending filename
	 * 
	 * @param src
	 *            path to the file
	 * @param inodesInPath
	 *            representing each of the components of src. The last INode is
	 *            the INode for the file.
	 * @throws QuotaExceededException
	 *             If addition of block exceeds space quota
	 */
	BlockInfo saveAllocatedBlock(String src, INodesInPath inodes,
			Block newBlock, DatanodeID[] targets) throws IOException {
		assert hasWriteLock();
		BlockInfo b = dir.addBlock(src, inodes, newBlock, targets);
		return b;
	}
}
