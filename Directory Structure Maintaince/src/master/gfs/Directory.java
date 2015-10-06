package master.gfs;

import java.io.Serializable;
import java.util.List;

/**
 * Class to represent the directory tree
 */
public class Directory implements Serializable {
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
	 * Access rights in 10 character string format
	 */
	private String 			accessRights;

	/**
	 * Size of a directory which is cumulative size of all the files and directories inside
	 * or individual size in case of a file
	 */
	private Long 			size;

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
	 * @param accessRights
	 *            Access string of 10 character format
	 * @param size
	 *            Size of the directory (cumulative size for directory) or a file
	 */
	public Directory(final String name, final boolean isFile, final List<Directory> children,
			final long modifiedTimeStamp, final String accessRights, final Long size) {
		this.name = name;
		this.isFile = isFile;
		this.children = children;
		this.modifiedTimeStamp = modifiedTimeStamp;
		this.accessRights = accessRights;
		this.size = size;
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
		for (Directory directory : root.children) {
			stringBuilder.append("\n" + print(directory, level + 1));
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
	 * @return the accessRights
	 */
	public String getAccessRights() {
		return accessRights;
	}

	/**
	 * @param accessRights the accessRights to set
	 */
	public void setAccessRights(final String accessRights) {
		this.accessRights = accessRights;
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
}