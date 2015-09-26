package master;

import java.util.List;

/**
 * Class to represent the directory tree
 *
 * @author Ekal.Golas
 */
public class Directory implements CharSequence {
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
			final StringBuilder output = new StringBuilder("Directory: " + name + "\n");
			if (children != null) {
				for (final Directory directory : children) {
					output.append("| " + directory + "\n");
				}
			}

			return output.toString();
		}
	}

	@Override
	public char charAt(final int index) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int length() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CharSequence subSequence(final int start, final int end) {
		// TODO Auto-generated method stub
		return null;
	}
}