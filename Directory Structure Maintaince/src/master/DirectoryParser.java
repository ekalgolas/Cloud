package master;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;

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
	
	private static HashMap<Integer, Directory> levelDirectoryMap = new HashMap<>();

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
		levelDirectoryMap.put(0, directory);

		try {
			Scanner scanner = new Scanner(new File(filePath));
			scanner.nextLine(); // Ignore first line
			while(scanner.hasNext()) {
				String line = scanner.nextLine();
				
				if(StringUtils.isBlank(line)) {
					System.out.println("End");
					break;
				}
				// Get current level
				int currentLevel = StringUtils.countMatches(line, "├")
						+ StringUtils.countMatches(line, "└")
						+ StringUtils.countMatches(line, "│");
				
				if(line.startsWith(" ")) {
					currentLevel++;
				}

				// Get the directory or file name - ignore the symbols
				String[] split = line.split(" ");
				String dirName = split[split.length - 1];
				String name = dirName;
				boolean isFile = true;
				if(dirName.endsWith("/")) {
					// Ends with '/' implies it is a directory and NOT a file
					name = dirName.substring(0, dirName.length() - 1);
					isFile = false;
				}
				
				// Create a new directory object
				Directory dir = new Directory();
				dir.isFile = isFile;
				dir.name = name;
				dir.children = new ArrayList<Directory>();
				System.out.println(dir);
				
				// Put current directory to current level
				levelDirectoryMap.put(currentLevel, dir);
				
				// Add the current node into children list of previous level node
				Directory parent = levelDirectoryMap.get(currentLevel - 1);
				parent.children.add(dir);
			}
			System.out.println();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		
		return directory;
	}
}