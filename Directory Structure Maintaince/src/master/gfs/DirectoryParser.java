package master.gfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;

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
	 * Mapping for directory and level of hierarchy
	 */
	private static HashMap<Integer, Directory>	levelDirectoryMap	= new HashMap<>();

	/**
	 * Parses a text file and creates the directory structure
	 *
	 * @param filePath
	 *            Path of the file to read
	 *            (output of 'tree -F -R -p -D --du --noreport' command)
	 * @return Directory structure as {@link Directory}
	 */
	public static Directory parseText(final String filePath) {
		// Create the root directory first
		final Directory directory = new Directory("root", false, new ArrayList<>());
		levelDirectoryMap.put(0, directory);

		try (final Scanner scanner = new Scanner(new File(filePath))) {
			// Ignore first line
			scanner.nextLine();

			// Read till EOF
			while (scanner.hasNext()) {
				// Read line. If empty, break
				final String line = scanner.nextLine();
				if (StringUtils.isBlank(line)) {
					break;
				}

				// Get current level
				int currentLevel = StringUtils.countMatches(line, "├") + StringUtils.countMatches(line, "└") + StringUtils.countMatches(line, "│");
				if (line.startsWith(" ")) {
					currentLevel++;
				}

				// Get the directory or file name - ignore the symbols
				final String[] split = line.split("]");
				final String dirName = split[split.length - 1].trim();

				// Figure out if it is a file or a directory
				String name = dirName;
				boolean isFile = true;
				if (dirName.endsWith("/")) {
					// Ends with '/' implies it is a directory and NOT a file
					name = dirName.substring(0, dirName.length() - 1);
					isFile = false;
				}

				// Extract other info from first part of the split
				String details = split[0].substring(StringUtils.lastIndexOf(split[0], "[") + 1);
				String accessRights = details.substring(0, 9);
				int timestampLength = 12;
				int timestampStartIndex = details.length() - timestampLength - 1;
				String readableTimeStamp = details.substring(timestampStartIndex).trim();
				String size = details.substring(10, timestampStartIndex - 2).trim();

				// Create a new directory object
				final Directory dir = new Directory(name, isFile, new ArrayList<>(),
						accessRights, readableTimeStamp, Long.parseLong(size));

				// Put current directory to current level
				levelDirectoryMap.put(currentLevel, dir);

				// Add the current node into children list of previous level node
				final Directory parent = levelDirectoryMap.get(currentLevel - 1);
				parent.getChildren().add(dir);
			}
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		}

		// Return the directory structure
		return directory;
	}
}