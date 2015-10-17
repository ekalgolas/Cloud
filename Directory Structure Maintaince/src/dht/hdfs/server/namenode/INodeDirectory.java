package dht.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class INodeDirectory extends INode {

	protected static final int DEFAULT_FILES_PER_DIRECTORY = 5;
	final static byte[] ROOT_NAME = "root".getBytes();

	private List<INode> children = null;

	public INodeDirectory(INode parentNode) {
		super(parentNode);
	}

	public INodeDirectory(long id, byte[] name) {
		super(null, id, name);
	}

	public static INodeDirectory valueOf(INode inode, Object path) throws Exception {
		if (inode == null) {
			throw new FileNotFoundException("Directory does not exist: ");
		}
		try {
			if (!inode.isDirectory()) {
				throw new Exception("Path is not a directory");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return inode.asDirectory();
	}

	/**
	 * Copy constructor
	 * 
	 * @param other
	 *            The INodeDirectory to be copied
	 * @param adopt
	 *            Indicate whether or not need to set the parent field of child
	 *            INodes to the new node
	 */
	public INodeDirectory(INodeDirectory other, boolean adopt) {
		super(other);
		this.children = other.children;
		if (adopt && this.children != null) {
			for (INode child : children) {
				child.setParent(this);
			}
		}
	}

	@Override
	public final boolean isDirectory() {
		return true;
	}

	@Override
	public final INodeDirectory asDirectory() {
		return this;
	}

	private int searchChildren(byte[] name) {
		return children == null ? -1 : Collections.binarySearch(children, name);
	}

	public boolean addChild(INode node) {
		final int low = searchChildren(node.getLocalNameBytes());
		if (low >= 0) {
			return false;
		}
		addChild(node, low);
		return true;
	}

	private void addChild(final INode node, final int insertionPoint) {
		if (children == null) {
			children = new ArrayList<INode>(DEFAULT_FILES_PER_DIRECTORY);
		}
		node.setParent(this);
		children.add(-insertionPoint - 1, node);

	}

	public void clearChildren() {
		this.children = null;
	}

	public INode getChild(byte[] name) {
		final ReadOnlyList<INode> c = getChildrenList();
		final int i = ReadOnlyList.Util.binarySearch(c, name);
		return i < 0 ? null : c.get(i);
	}

	public ReadOnlyList<INode> getChildrenList() {
		return children == null ? ReadOnlyList.Util.<INode> emptyList() : ReadOnlyList.Util.asReadOnlyList(children);
	}

	INode getNode(String path, boolean resolveLink) throws Exception {
		return getLastINodeInPath(path, resolveLink).getINode(0);
	}

	INodesInPath getLastINodeInPath(String path, boolean resolveLink) throws Exception {
		return INodesInPath.resolve(this, getPathComponents(path), 1, resolveLink);
	}

	/**
	 * @return the INodesInPath of the components in src
	 * @throws UnresolvedLinkException
	 *             if symlink can't be resolved
	 * @throws SnapshotAccessControlException
	 *             if path is in RO snapshot
	 */
	INodesInPath getINodesInPath4Write(String src, boolean resolveLink) throws Exception {
		final byte[][] components = INode.getPathComponents(src);
		INodesInPath inodesInPath = INodesInPath.resolve(this, components, components.length, resolveLink);
		return inodesInPath;
	}

}
