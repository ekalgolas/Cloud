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
	public String			name;

	/**
	 * Whether the element is a file or not
	 */
	public boolean			isFile;

	/**
	 * List of child directories or files in this directory
	 */
	public List<Directory>	children;

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (isFile) {
			return "File: " + name;
		} else {
			return "Directory: " + name;
		}
	}
}