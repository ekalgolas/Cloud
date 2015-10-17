/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dht.hdfs.server.namenode;

/**
 * Contains INodes information resolved from a given path.
 */
public class INodesInPath {

	/**
	 * Given some components, create a path name.
	 * 
	 * @param components
	 *            The path components
	 * @param start
	 *            index
	 * @param end
	 *            index
	 * @return concatenated path
	 */
	private static String constructPath(byte[][] components, int start, int end) {
		StringBuilder buf = new StringBuilder();
		for (int i = start; i < end; i++) {
			buf.append(new String(components[i]));
			if (i < end - 1) {
				buf.append(HdfsPath.separator);
			}
		}
		return buf.toString();
	}

	static INodesInPath resolve(final INodeDirectory startingDir, final byte[][] components) throws Exception {
		return resolve(startingDir, components, components.length, false);
	}

	/**
	 * Retrieve existing INodes from a path. If existing is big enough to store
	 * all path components (existing and non-existing), then existing INodes
	 * will be stored starting from the root INode into existing[0]; if existing
	 * is not big enough to store all path components, then only the last
	 * existing and non existing INodes will be stored so that
	 * existing[existing.length-1] refers to the INode of the final component.
	 * 
	 * An UnresolvedPathException is always thrown when an intermediate path
	 * component refers to a symbolic link. If the final path component refers
	 * to a symbolic link then an UnresolvedPathException is only thrown if
	 * resolveLink is true.
	 * 
	 * <p>
	 * Example: <br>
	 * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
	 * following path components: ["","c1","c2","c3"],
	 * 
	 * <p>
	 * <code>getExistingPathINodes(["","c1","c2"], [?])</code> should fill the
	 * array with [c2] <br>
	 * <code>getExistingPathINodes(["","c1","c2","c3"], [?])</code> should fill
	 * the array with [null]
	 * 
	 * <p>
	 * <code>getExistingPathINodes(["","c1","c2"], [?,?])</code> should fill the
	 * array with [c1,c2] <br>
	 * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?])</code> should
	 * fill the array with [c2,null]
	 * 
	 * <p>
	 * <code>getExistingPathINodes(["","c1","c2"], [?,?,?,?])</code> should fill
	 * the array with [rootINode,c1,c2,null], <br>
	 * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?,?,?])</code> should
	 * fill the array with [rootINode,c1,c2,null]
	 * 
	 * @param startingDir
	 *            the starting directory
	 * @param components
	 *            array of path component name
	 * @param numOfINodes
	 *            number of INodes to return
	 * @param resolveLink
	 *            indicates whether UnresolvedLinkException should be thrown
	 *            when the path refers to a symbolic link.
	 * @return the specified number of existing INodes in the path
	 */
	static INodesInPath resolve(final INodeDirectory startingDir, final byte[][] components, final int numOfINodes,
			final boolean resolveLink) throws Exception {

		INode curNode = startingDir;
		final INodesInPath existing = new INodesInPath(components, numOfINodes);
		int count = 0;
		int index = numOfINodes - components.length;
		if (index > 0) {
			index = 0;
		}

		while (count < components.length && curNode != null) {
			final boolean lastComp = (count == components.length - 1);
			if (index >= 0) {
				existing.addNode(curNode);
			}
			final boolean isDir = curNode.isDirectory();
			final INodeDirectory dir = isDir ? curNode.asDirectory() : null;

			if (lastComp || !isDir) {
				break;
			}
			final byte[] childName = components[count + 1];

			curNode = dir.getChild(childName);

			count++;
			index++;
		}
		return existing;
	}

	private final byte[][] path;
	/**
	 * Array with the specified number of INodes resolved for a given path.
	 */
	private INode[] inodes;
	/**
	 * Indicate the number of non-null elements in {@link #inodes}
	 */
	private int numNonNull;
	/**
	 * The path for a snapshot file/dir contains the .snapshot thus makes the
	 * length of the path components larger the number of inodes. We use the
	 * capacity to control this special case.
	 */
	private int capacity;

	private INodesInPath(byte[][] path, int number) {
		this.path = path;
		assert(number >= 0);
		inodes = new INode[number];
		capacity = number;
		numNonNull = 0;
	}

	/**
	 * @return the whole inodes array including the null elements.
	 */
	INode[] getINodes() {
		if (capacity < inodes.length) {
			INode[] newNodes = new INode[capacity];
			System.arraycopy(inodes, 0, newNodes, 0, capacity);
			inodes = newNodes;
		}
		return inodes;
	}

	/**
	 * @return the i-th inode if i >= 0; otherwise, i < 0, return the (length +
	 *         i)-th inode.
	 */
	public INode getINode(int i) {
		return inodes[i >= 0 ? i : inodes.length + i];
	}

	/**
	 * @return the last inode.
	 */
	public INode getLastINode() {
		return inodes[inodes.length - 1];
	}

	byte[] getLastLocalName() {
		return path[path.length - 1];
	}

	/**
	 * Add an INode at the end of the array
	 */
	private void addNode(INode node) {
		inodes[numNonNull++] = node;
	}

	void setINode(int i, INode inode) {
		inodes[i >= 0 ? i : inodes.length + i] = inode;
	}

	void setLastINode(INode last) {
		inodes[inodes.length - 1] = last;
	}

	/**
	 * @return The number of non-null elements
	 */
	int getNumNonNull() {
		return numNonNull;
	}

	private static String toString(INode inode) {
		return inode == null ? null : inode.getLocalName();
	}

	public String toString() {
		final StringBuilder b = new StringBuilder(getClass().getSimpleName()).append(": path = ")
				.append(path.toString()).append("\n  inodes = ");
		if (inodes == null) {
			b.append("null");
		} else if (inodes.length == 0) {
			b.append("[]");
		} else {
			b.append("[").append(toString(inodes[0]));
			for (int i = 1; i < inodes.length; i++) {
				b.append(", ").append(toString(inodes[i]));
			}
			b.append("], length=").append(inodes.length);
		}
		b.append("\n  numNonNull = ").append(numNonNull).append("\n  capacity   = ").append(capacity);
		return b.toString();
	}
}