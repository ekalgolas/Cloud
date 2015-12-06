package commons.dir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * <pre>
 * Class to implement parsing of the directory structure from a text file
 * 	1. Read the text file
 * 	2. Get the directory names
 * 	3. Create the B-Tree directory structure
 * </pre>
 *
 */
public class DirectoryParser {
	/**
	 * Constants based on the output format of
	 * 'tree -F -R -p --timefmt "%s" --du --noreport' command
	 */

    /**
	 * Mapping for directory and level of hierarchy
	 */
	private static HashMap<Integer, Directory>	levelDirectoryMap		= new HashMap<>();

	private final static Logger						LOGGER					= Logger.getLogger(DirectoryParser.class);

	/**
	 * Parses a text file and creates the directory structure
	 *
	 * @param filePath
	 *            Path of the file to read
	 *            (output of 'tree -F -R -p --timefmt "%s" --du --noreport' command)
	 * @return Directory structure as {@link Directory}
	 */
	public static Directory parseText(final String filePath) {
		// Create the root directory first
		final Directory directory = new Directory("root", false, new ArrayList<>());
		directory.setSize(0L);
		levelDirectoryMap.put(0, directory);


		try {
			// Read till EOF
			List<String> lines = FileUtils.readLines(new File(filePath));

			for (String line : lines) {

				if (StringUtils.isBlank(line)) {
					break;
				}

				// Calculate current level
				int currentLevel = StringUtils.countMatches(line, "├")
						+ StringUtils.countMatches(line, "└")
						+ StringUtils.countMatches(line, "│") + 1;
				if (line.startsWith(" ")) {
					currentLevel++;
				}

				Directory dir = null;
				if (currentLevel == 1) {
					// A new root is found in the input, make it child of the
					// super root
					dir = new Directory(line, false, new ArrayList<>(), 0L, 0L);
				} else {
					// Get the directory or file name - ignore the symbols
					final String[] split = line.split("]");
					final String dirName = split[split.length - 1].trim();

					// Figure out if it is a file or a directory
					String name = dirName;
					boolean isFile = true;
					if (dirName.endsWith("/") || dirName.endsWith("\\")) {
						// Ends with '/' implies it is a directory and NOT a
						// file
						name = dirName.substring(0, dirName.length() - 1);
						isFile = false;
					}

					// Extract other info from first part of the split
					final String details = split[0].substring(
							StringUtils.lastIndexOf(split[0], "[") + 1).trim();
					final String[] detail = details.split(" ");
					final String size = detail[0].trim();
					final String readableTimeStamp = detail[detail.length - 1]
							.trim();

					// Create a new directory object
					dir = new Directory(name, isFile, new ArrayList<>(),
							Long.parseLong(readableTimeStamp),
							Long.parseLong(size));
				}

				// Put current directory to current level
				levelDirectoryMap.put(currentLevel, dir);

				// Add the current node into children list of previous level
				// node
				final Directory parent = levelDirectoryMap
						.get(currentLevel - 1);
				parent.getChildren().add(dir);
			}
		} catch (final IOException e) {
			LOGGER.error("", e);
		}

		// Return the directory structure
		return directory;
	}
}