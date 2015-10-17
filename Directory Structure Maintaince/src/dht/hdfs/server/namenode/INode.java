package dht.hdfs.server.namenode;

import com.google.common.primitives.SignedBytes;

public class INode implements Diff.Element<byte[]> {
	private INode parent = null;
	public String userName;
	private long id;
	private byte[] name = null;
	public static final byte[] EMPTY_BYTES = {};

	public INode(INode parentNode) {
		parent = parentNode;
	}

	public INode(INode parent, long id, byte[] name) {
		this.parent = parent;
		this.id = id;
		this.name = name;
	}

	final boolean isRoot() {
		return getLocalNameBytes().length == 0;
	}

	public final boolean isAncestorDirectory(final INodeDirectory dir) {
		for (INodeDirectory p = getParent(); p != null; p = p.getParent()) {
			if (p == dir) {
				return true;
			}
		}
		return false;
	}

	public final INodeDirectory getParent() {
		// return parent == null ? null
		// : parent.isReference() ? getParentReference().getParent()
		// : parent.asDirectory();
		return null;
	}

	public boolean isReference() {
		return false;
	}

	public boolean isFile() {
		return false;
	}

	public boolean isDirectory() {
		return false;
	}

	public INodeDirectory asDirectory() {
		throw new IllegalStateException("Current inode is not a directory: ");
	}

	public INodeFile asFile() {
		throw new IllegalStateException("Current inode is not a file: ");
	}

	public final String getLocalName() {
		final byte[] name = getLocalNameBytes();
		return name == null ? null : new String(name);
	}

	public final byte[] getKey() {
		return getLocalNameBytes();
	}

	public String getFullPathName() {
		// Get the full path name of this inode.
		// return FSDirectory.getFullPathName(this);
		return null;
	}

	public void setParent(INodeDirectory parent) {
		this.parent = parent;
	}

	static String[] getPathNames(String path) {

		if (path == null || !path.startsWith(HdfsPath.separator)) {
			throw new AssertionError("Absolute path required");
		}

		String[] split = path.split("/");

		return split;
	}

	@Override
	public final boolean equals(Object that) {
		if (this == that) {
			return true;
		}
		if (that == null || !(that instanceof INode)) {
			return false;
		}
		return getId() == ((INode) that).getId();
	}

	@Override
	public final int hashCode() {
		long id = getId();
		return (int) (id ^ (id >>> 32));
	}

	public long getId() {
		return this.id;
	}

	public final byte[] getLocalNameBytes() {
		return name;
	}

	public final void setLocalName(byte[] name) {
		this.name = name;
	}

	void setUser(String user) {
		userName = user;

	}

	static byte[][] getPathComponents(String[] strings) {
		if (strings.length == 0) {
			return new byte[][] { null };
		}
		byte[][] bytes = new byte[strings.length][];
		for (int i = 0; i < strings.length; i++)
			bytes[i] = strings[i].getBytes();
		return bytes;
	}

	static byte[][] getPathComponents(String path) {
		return getPathComponents(getPathNames(path));
	}

	@Override
	public final int compareTo(byte[] bytes) {
		byte[] left = getLocalNameBytes();
		byte[] right = bytes;

		if (left == null) {
			left = EMPTY_BYTES;
		}
		if (right == null) {
			right = EMPTY_BYTES;
		}
		return SignedBytes.lexicographicalComparator().compare(left, right);
	}
}
