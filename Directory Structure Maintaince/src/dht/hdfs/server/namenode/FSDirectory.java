package dht.hdfs.server.namenode;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import dht.hdfs.server.common.HdfsServerConstants.BlockUCState;
import dht.hdfs.server.protocol.DatanodeID;

public class FSDirectory implements Closeable {

	private static INodeDirectory createRoot() {
		INodeDirectory r = new INodeDirectory(INodeId.ROOT_INODE_ID, INodeDirectory.ROOT_NAME);
		return r;
	}

	private final FSNamesystem namesystem;
	private INodeDirectory rootDir;
	private ReentrantReadWriteLock dirLock;
	private Condition cond;
	private volatile boolean ready = false;

	public FSDirectory(FSNamesystem namesystem) {
		this.dirLock = new ReentrantReadWriteLock(true);
		this.cond = dirLock.writeLock().newCondition();
		rootDir = createRoot();
		this.namesystem = namesystem;
	}

	void readLock() {
		this.dirLock.readLock().lock();
	}

	void readUnlock() {
		this.dirLock.readLock().unlock();
	}

	void writeLock() {
		this.dirLock.writeLock().lock();
	}

	void writeUnlock() {
		this.dirLock.writeLock().unlock();
	}

	boolean hasWriteLock() {
		return this.dirLock.isWriteLockedByCurrentThread();
	}

	boolean hasReadLock() {
		return this.dirLock.getReadHoldCount() > 0;
	}

	private FSNamesystem getFSNamesystem() {
		return namesystem;
	}

	public INodeDirectory getRoot() {
		return rootDir;
	}

	INodeFile addFile(String path, short replication, long preferredBlockSize, String clientName, String clientMachine)
			throws Exception {

		HdfsPath parent = new HdfsPath(path).getDir();
		if (parent == null) {
			return null;
		}

		if (!mkdirs(parent.getPath())) {
			return null;
		}

		INodeFile newNode = new INodeFile(namesystem.allocateNewInodeId(), null, BlockInfo.EMPTY_ARRAY, replication,
				preferredBlockSize);
		boolean added = false;
		writeLock();
		try {
			added = addINode(path, newNode);
		} finally {
			writeUnlock();
		}

		return newNode;
	}

	INodeFile unprotectedAddFile(long id, String path, short replication, long modificationTime, long atime,
			long preferredBlockSize, boolean underConstruction, String clientName, String clientMachine)
					throws Exception {
		final INodeFile newNode;
		assert hasWriteLock();

		newNode = new INodeFile(id, null);

		try {
			if (addINode(path, newNode)) {
				return newNode;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	void closeFile(String path, INodeFile file) {
		waitForReady();
		writeLock();
		writeUnlock();
	}

	/**
	 * Create a directory If ancestor directories do not exist, automatically
	 * create them.
	 * 
	 * @param src
	 *            string representation of the path to the directory
	 * @param permissions
	 *            the permission of the directory
	 * @param isAutocreate
	 *            if the permission of the directory should inherit from its
	 *            parent or not. u+wx is implicitly added to the automatically
	 *            created directories, and to the given directory if
	 *            inheritPermission is true
	 * @param now
	 *            creation time
	 * @return true if the operation succeeds false otherwise
	 * @throws FileNotFoundException
	 *             if an ancestor or itself is a file
	 * @throws QuotaExceededException
	 *             if directory creation violates any quota limit
	 * @throws UnresolvedLinkException
	 *             if a symlink is encountered in src.
	 * @throws SnapshotAccessControlException
	 *             if path is in RO snapshot
	 */
	boolean mkdirs(String src) throws Exception {
		src = normalizePath(src);
		String[] names = INode.getPathNames(src);
		byte[][] components = INode.getPathComponents(names);
		final int lastInodeIndex = components.length - 1;

		writeLock();
		try {
			INodesInPath iip = getExistingPathINodes(components);

			INode[] inodes = iip.getINodes();

			// find the index of the first null in inodes[]
			StringBuilder pathbuilder = new StringBuilder();
			int i = 1;
			for (; i < inodes.length && inodes[i] != null; i++) {
				pathbuilder.append(HdfsPath.separator).append(names[i]);
				if (!inodes[i].isDirectory()) {
					throw new Exception(
							"Parent path is not a directory: " + pathbuilder + " " + inodes[i].getLocalName());
				}
			}

			// create directories beginning from the first null index
			for (; i < inodes.length; i++) {
				pathbuilder.append(HdfsPath.separator + names[i]);
				unprotectedMkdir(namesystem.allocateNewInodeId(), iip, i, components[i]);
				if (inodes[i] == null) {
					return false;
				}
			}
		} finally {
			writeUnlock();
		}
		return true;
	}

	/**
	 * create a directory at index pos. The parent path to the directory is at
	 * [0, pos-1]. All ancestors exist. Newly created one stored at index pos.
	 */
	private void unprotectedMkdir(long inodeId, INodesInPath inodesInPath, int pos, byte[] name) throws Exception {
		assert hasWriteLock();
		final INodeDirectory dir = new INodeDirectory(inodeId, name);
		if (addChild(inodesInPath, pos, dir)) {
			inodesInPath.setINode(pos, dir);
		}
	}

	String normalizePath(String src) {
		if (src.length() > 1 && src.endsWith("/")) {
			src = src.substring(0, src.length() - 1);
		}
		return src;
	}

	INodesInPath getExistingPathINodes(byte[][] components) throws Exception {
		return INodesInPath.resolve(rootDir, components);
	}

	/**
	 * Add a node child to the inodes at index pos. Its ancestors are stored at
	 * [0, pos-1].
	 * 
	 * @return false if the child with this name already exists; otherwise
	 *         return true;
	 * @throw QuotaExceededException is thrown if it violates quota limit
	 */
	private boolean addChild(INodesInPath iip, int pos, INode child) throws Exception {
		final INode[] inodes = iip.getINodes();

		if (pos == 0 && inodes[0] == rootDir) {
			throw new Exception("File name \"" + child.getLocalName() + "\" is reserved and cannot "
					+ "be created. If this is during upgrade change the name of the "
					+ "existing file or directory to another name before upgrading " + "to the new release.");
		}

		final INodeDirectory parent = inodes[pos - 1].asDirectory();
		boolean added = false;
		try {
			added = parent.addChild(child);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

		return added;
	}

	private boolean addINode(String src, INode child) throws Exception {
		byte[][] components = INode.getPathComponents(src);
		child.setLocalName(components[components.length - 1]);
		writeLock();
		try {
			return addLastINode(getExistingPathINodes(components), child);
		} finally {
			writeUnlock();
		}
	}

	private boolean addLastINode(INodesInPath inodesInPath, INode inode) throws Exception {
		final int pos = inodesInPath.getINodes().length - 1;
		return addChild(inodesInPath, pos, inode);
	}

	void waitForReady() {
		if (!ready) {
			writeLock();
			try {
				while (!ready) {
					try {
						cond.await(5000, TimeUnit.MILLISECONDS);
					} catch (InterruptedException ie) {
					}
				}
			} finally {
				writeUnlock();
			}
		}
	}

	boolean isReady() {
		return ready;
	}

	// exposed for unit tests
	protected void setReady(boolean flag) {
		ready = flag;
	}

	boolean exists(String src) throws Exception {
		src = normalizePath(src);
		readLock();
		try {
			INode inode = rootDir.getNode(src, false);
			if (inode == null) {
				return false;
			}
			return !inode.isFile() || inode.asFile().getBlocks() != null;
		} finally {
			readUnlock();
		}
	}

	/**
	 * Remove a block from the file.
	 * 
	 * @return Whether the block exists in the corresponding file
	 */
	boolean removeBlock(String path, INodeFile fileNode, Block block) throws IOException {
		waitForReady();

		writeLock();
		try {
			return unprotectedRemoveBlock(path, fileNode, block);
		} finally {
			writeUnlock();
		}
	}

	boolean unprotectedRemoveBlock(String path, INodeFile fileNode, Block block) throws IOException {
		// modify file-> block and blocksMap
		boolean removed = fileNode.removeLastBlock(block);
		if (!removed) {
			return false;
		}
		return true;
	}

	/**
	 * Set file replication
	 * 
	 * @param src
	 *            file name
	 * @param replication
	 *            new replication
	 * @param blockRepls
	 *            block replications - output parameter
	 * @return array of file blocks
	 * @throws QuotaExceededException
	 * @throws SnapshotAccessControlException
	 */
	// Block[] setReplication(String src, short replication, short[] blockRepls)
	// throws Exception {
	// waitForReady();
	// writeLock();
	// try {
	// final Block[] fileBlocks = unprotectedSetReplication(src,
	// replication, blockRepls);
	// return fileBlocks;
	// } finally {
	// writeUnlock();
	// }
	// }
	//
	// Block[] unprotectedSetReplication(String src, short replication,
	// short[] blockRepls) throws Exception {
	// assert hasWriteLock();
	//
	// final INodesInPath iip = rootDir.getINodesInPath4Write(src, true);
	// final INode inode = iip.getLastINode();
	// if (inode == null || !inode.isFile()) {
	// return null;
	// }
	// INodeFile file = inode.asFile();
	// final short oldBR = file.getBlockReplication();
	//
	// file = file.setFileReplication(replication, iip.getLatestSnapshot(),
	// inodeMap);
	// return file.getBlocks();
	// }

	long getPreferredBlockSize(String path) throws Exception, FileNotFoundException, IOException {
		readLock();
		try {
			return INodeFile.valueOf(rootDir.getNode(path, false), path).getPreferredBlockSize();
		} finally {
			readUnlock();
		}
	}

	/**
	 * Get the blocks associated with the file.
	 */
	Block[] getFileBlocks(String src) throws Exception {
		waitForReady();
		readLock();
		try {
			final INode i = rootDir.getNode(src, false);
			return i != null && i.isFile() ? i.asFile().getBlocks() : null;
		} finally {
			readUnlock();
		}
	}

	public INode getINode(String src) throws Exception {
		return getLastINodeInPath(src).getINode(0);
	}

	public INodesInPath getLastINodeInPath(String src) throws Exception {
		readLock();
		try {
			return rootDir.getLastINodeInPath(src, true);
		} finally {
			readUnlock();
		}
	}

	boolean isDir(String src) throws Exception {
		src = normalizePath(src);
		readLock();
		try {
			INode node = rootDir.getNode(src, false);
			return node != null && node.isDirectory();
		} finally {
			readUnlock();
		}
	}

	private boolean addLastINode(INodesInPath inodesInPath, INode inode, boolean checkQuota) throws Exception {
		final int pos = inodesInPath.getINodes().length - 1;
		return addChild(inodesInPath, pos, inode);
	}

	void reset() {
		writeLock();
		try {
			setReady(false);
			rootDir = createRoot();
		} finally {
			writeUnlock();
		}
	}

	public INodesInPath getINodesInPath4Write(String src) throws Exception {
		readLock();
		try {
			return rootDir.getINodesInPath4Write(src, true);
		} finally {
			readUnlock();
		}
	}

	boolean isNonEmptyDirectory(String path) throws Exception {
		readLock();
		try {
			final INodesInPath inodesInPath = rootDir.getLastINodeInPath(path, false);
			final INode inode = inodesInPath.getINode(0);
			if (inode == null || !inode.isDirectory()) {
				// not found or not a directory
				return false;
			}

			return !inode.asDirectory().getChildrenList().isEmpty();
		} finally {
			readUnlock();
		}
	}

	HdfsFileStatus getFileInfo(String src, boolean resolveLink) throws Exception {
		String srcs = normalizePath(src);
		readLock();
		try {

			final INodesInPath inodesInPath = rootDir.getLastINodeInPath(srcs, false);
			final INode i = inodesInPath.getINode(0);
			return i == null ? null : createFileStatus(HdfsFileStatus.EMPTY_NAME, i);
		} finally {
			readUnlock();
		}
	}

	HdfsFileStatus createFileStatus(byte[] path, INode node) {
		long size = 0; // length is zero for directories
		short replication = 0;
		long blocksize = 0;
		if (node.isFile()) {
			final INodeFile fileNode = node.asFile();
			size = 11;
			replication = fileNode.getFileReplication();
			blocksize = fileNode.getPreferredBlockSize();
		}
		int childrenNum = 0;

		return new HdfsFileStatus(size, node.isDirectory(), replication, blocksize, 0, 0, "user1", "group1", null, path,
				node.getId(), childrenNum);
	}

	public void dump() {
		printTree(rootDir, 0);
	}

	private void printTree(INodeDirectory root, int level) {
		ReadOnlyList<INode> childrens = root.getChildrenList();

		for (int i = 0; i < level; i++) {
			System.out.print("|");
		}

		System.out.println(root.getId() + "," + root.getLocalName());
		for (INode child : childrens) {
			if (child.isDirectory()) {
				printTree(child.asDirectory(), ++level);
			}
		}
	}

	@Override
	public void close() throws IOException {

	}

	// New Added
	/**
	 * Add a block to the file. Returns a reference to the added block.
	 */

	BlockInfo addBlock(String path, INodesInPath inodesInPath, Block block, DatanodeID targets[]) throws IOException {
		waitForReady();

		writeLock();
		try {
			final INodeFileUnderConstruction fileINode = INodeFileUnderConstruction.valueOf(inodesInPath.getLastINode(),
					path);

			// associate new last block for the file
			BlockInfoUnderConstruction blockInfo = new BlockInfoUnderConstruction(block, fileINode.getFileReplication(),
					BlockUCState.UNDER_CONSTRUCTION, targets);
			// getBlockManager().addBlockCollection(blockInfo, fileINode);
			// fileINode.addBlock(blockInfo);

			return blockInfo;
		} finally {
			writeUnlock();
		}
	}
}
