package master;

import java.util.List;

/**
 * Class to represent the directory tree
 *
 * @author Ekal.Golas
 */
public class Directory {
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
		for (int j = 0; j < root.children.size(); j++) {
			if (root.children.get(j) != null) {
				stringBuilder.append("\n" + print(root.children.get(j), level + 1));
			}
		}

		// Return the string representation
		return stringBuilder.toString();
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
}