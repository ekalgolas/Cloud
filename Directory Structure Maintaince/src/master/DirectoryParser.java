package master;

import java.util.ArrayList;

/**
 * <pre>
 * Class to implement parsing of the directory structure from a text file
 * 	1. Read the text file
 * 	2. Get the directory names
 * 	3. Create the B-Tree directory structure
 * </pre>
 *
 * @author Ekal.Golas
 */
public class DirectoryParser {
	/**
	 * Parses a text file and creates the directory structure
	 *
	 * @param filePath
	 *            Path of the file to read
	 * @return Directory structure as {@link Directory}
	 */
	public static Directory parseText(final String filePath) {
		final Directory directory = new Directory();
		directory.name = "root";
		directory.isFile = false;
		directory.children = new ArrayList<>();

		return directory;
	}
}