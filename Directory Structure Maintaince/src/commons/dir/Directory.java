package commons.dir;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import master.metadata.Inode;

/**
 * Class to represent the directory tree
 */
public class Directory implements Serializable, Cloneable {
	/**
	 * Default serial version UID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Name of the directory
	 */
	private String			name;

	/**
	 * Whether the element is a file or not
	 */
	private boolean			isFile;

	/**
	 * List of child directories or files in this directory
	 */
	private List<Directory>	children;

	/**
	 * Last Modified Time
	 */
	private Long 			modifiedTimeStamp;

	/**
	 * Size of a directory which is cumulative size of all the files and directories inside
	 * or individual size in case of a file
	 */
	private Long 			size;

	/**
	 * Contains the inode informations like inode number, MDS details.
	 */
	private Inode			inode;

	/**
	 * Counter to keep track of the # of access to this directory.
	 */
	private long 			operationCounter;

	/**
	 * Reentrant Lock for the directory;
	 */
	private ReentrantReadWriteLock lock;
	
	/**
	 * Read lock on the directory
	 */
	private ReadLock readLock;

	/**
     * Write lock on the directory
     */
	private WriteLock writeLock;

	/**
	 * Constructor
	 *
	 * @param name
	 *            Name of the file/directory
	 * @param isFile
	 *            True if a file, else false
	 * @param children
	 *            List of sub-directories
	 */
	public Directory(final String name, final boolean isFile, final List<Directory> children) {
		this.name = name;
		this.isFile = isFile;
		this.children = children;
		setupLocks();
	}

	/**
	 * Constructor
	 *
	 * @param name
	 *            Name of the file/directory
	 * @param isFile
	 *            True if a file, else false
	 * @param children
	 *            List of sub-directories
	 * @param modifiedDateTime
	 *            Modification date and time in human readable format
	 * @param size
	 *            Size of the directory (cumulative size for directory) or a file
	 */
	public Directory(final String name, final boolean isFile, final List<Directory> children,
			final long modifiedTimeStamp, final Long size) {
		this.name = name;
		this.isFile = isFile;
		this.children = children;
		this.modifiedTimeStamp = modifiedTimeStamp;
		this.size = size;
		setupLocks();
	}

	/**
	 * Initialize read and write lock objects
	 */
	private void setupLocks() {
	    lock = new ReentrantReadWriteLock();
	    readLock = lock.readLock();
	    writeLock = lock.writeLock();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		// Call print with level 0 for this directory
		return print(this, 0);
	}

	/**
	 * Calculates string representation the directory structure
	 *
	 * @param root
	 *            Root node of the subtree
	 * @param level
	 *            Level of the node
	 * @return String representation of the tree
	 */
	private String print(final Directory root, final int level) {
		// Initialize string builder and append tabs according to the level of
		// the node
		final StringBuilder stringBuilder = new StringBuilder();

		// Append tabs for indentation according to level
		for (int i = 0; i < level; i++) {
			stringBuilder.append("| ");
		}

		// Write the details of file or directory as this node
		stringBuilder.append((root.isFile ? "File: " : "Directory: ") + root.name);

		// Do for all sub-directories
		if (root.children != null) {
			for (final Directory directory : root.children) {
				stringBuilder.append("\n" + print(directory, level + 1));
			}
		}

		// Return the string representation
		return stringBuilder.toString();
	}

	public boolean isEmptyDirectory() {
		return !isFile && children.isEmpty();
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(final String name) {
		this.name = name;
	}

	/**
	 * @return the isFile
	 */
	public boolean isFile() {
		return isFile;
	}

	/**
	 * @param isFile
	 *            the isFile to set
	 */
	public void setFile(final boolean isFile) {
		this.isFile = isFile;
	}

	/**
	 * @return the children
	 */
	public List<Directory> getChildren() {
		return children;
	}

	/**
	 * @param children
	 *            the children to set
	 */
	public void setChildren(final List<Directory> children) {
		this.children = children;
	}

	/**
	 * @return The time stamp
	 */
	public Long getModifiedTimeStamp() {
		return modifiedTimeStamp;
	}

	/**
	 * @param modifiedTimeStamp
	 *            The time stamp
	 */
	public void setModifiedTimeStamp(final Long modifiedTimeStamp) {
		this.modifiedTimeStamp = modifiedTimeStamp;
	}

	/**
	 * @return the size
	 */
	public Long getSize() {
		return size;
	}

	/**
	 * @param size the size to set
	 */
	public void setSize(final Long size) {
		this.size = size;
	}

	/**
	 * Get the associated inode
	 * @return inode
	 */
	public Inode getInode() {
		return inode;
	}

	/**
	 * Set the inode for this directory.
	 * @param inode
	 */
	public void setInode(final Inode inode) {
		this.inode = inode;
	}

	/**
	 * Get the total operation counter.
	 * @return counter
	 */
	public long getOperationCounter() {
		return operationCounter;
	}

	/**
	 * Set the total operation counter.
	 * @param operationCounter
	 */
	public void setOperationCounter(final long operationCounter) {
		this.operationCounter = operationCounter;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, isFile);
	}

	@Override
	public boolean equals(final Object other) {
		if(!(other instanceof Directory)) {
			return false;
		}

		final Directory that = (Directory) other;
		return name.equals(that.name)
			&& isFile == that.isFile;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

    /**
     * @return the readLock
     */
    public ReadLock getReadLock() {
        return readLock;
    }

    /**
     * @return the writeLock
     */
    public WriteLock getWriteLock() {
        return writeLock;
    }

}