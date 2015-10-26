package master.dht;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

import master.dht.dhtfs.client.ClientConfiguration;
import master.dht.dhtfs.client.DHTFileSystem;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.io.Files;
import commons.AppConfig;
import commons.dir.Directory;
import commons.dir.DirectoryParser;

/**
 * Parses DHT directory structure
 *
 * @author sahith
 */
public class DhtDirectoryParser extends DirectoryParser {
	private final static Logger					LOGGER				= Logger.getLogger(DhtDirectoryParser.class);

	/**
	 * Mapping for directory and level of hierarchy
	 */
	private static HashMap<Integer, Directory>	levelDirectoryMap	= new HashMap<>();

	/**
	 * Parses the pre-configured input file in the server to construct DHT directory structure
	 *
	 * @param cutLevel
	 *            Cut level for DHT
	 * @return Map of paths to DHT files
	 * @throws IOException
	 */
	public static HashMap<String, File> parseText(final int cutLevel)
			throws IOException {
		// Initialize DHT
		ClientConfiguration.initialize("conf/dhtclient.conf");
		final DHTFileSystem fileSystem = new DHTFileSystem();
		fileSystem.initialize();

		// Get a temp folder
		final File folder = Files.createTempDir();
		final File root = new File(folder.getAbsolutePath() + "/root.txt");
		folder.deleteOnExit();

		// Put root file initially
		final Directory rootDirectory = new Directory("root", false, new ArrayList<>());
		levelDirectoryMap.put(0, rootDirectory);
		final HashMap<String, File> fileMap = new HashMap<>();
		fileMap.put("root", root);

		// For each level, parse
		try (final Scanner scanner = new Scanner(new File(AppConfig.getValue("server.inputFile")))) {
			// Ignore first line
			scanner.nextLine();

			// Read till EOF
			while (scanner.hasNext()) {
				// Read line. If empty, break
				final String line = scanner.nextLine();
				if (StringUtils.isBlank(line)) {
					break;
				}

				// Calculate current level
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
				final String details = split[0].substring(StringUtils.lastIndexOf(split[0], "[") + 1);
				final String accessRights = details.substring(0, ACCESS_RIGHT_ENDINDEX);
				final int sizeEndIndex = details.lastIndexOf(" ");
				final String readableTimeStamp = details.substring(sizeEndIndex).trim();
				final String size = details.substring(SIZE_STARTINDEX, sizeEndIndex).trim();

				// Create a new directory object handling spaces
				final Directory directory = new Directory(name.replace(" ", "-"),
						isFile,
						new ArrayList<>(),
						Long.parseLong(readableTimeStamp),
						accessRights,
						Long.parseLong(size));

				// Put current directory to current level
				levelDirectoryMap.put(currentLevel, directory);

				// Add the current node into children list of previous level node
				final Directory parent = levelDirectoryMap.get(currentLevel - 1);
				parent.getChildren().add(directory);

				String pathName = "", filename = "";
				File levelfile = null, previouslevelfile = null;

				// If this level needs to be in different cut file
				if (currentLevel % cutLevel == 0) {
					// Cut the path and get a file name delimited by '+'
					for (int n = 0; n <= currentLevel; n++) {
						final Directory node = levelDirectoryMap.get(n);
						filename = filename.concat(node.getName());
						if (n == currentLevel) {
							break;
						}

						filename = filename.concat("+");
					}

					// Create a level file with the cut path
					levelfile = new File(folder.getAbsolutePath() + "/" + filename + ".txt");
					if (currentLevel > 0) {
						// Get the parent path
						String ppathName = "";
						for (int b = currentLevel - cutLevel; b < currentLevel; b++) {
							final Directory node = levelDirectoryMap.get(b);
							ppathName = ppathName.concat(node.getName());
							ppathName = ppathName.concat("/");
						}

						ppathName = ppathName.concat(directory.getName());

						// Get the parent file and update it
						final String[] paths = levelfile.getName().replace("+", "/").split("/");
						final String pPath = StringUtils.join(Arrays.asList(paths)
								.subList(0, paths.length - cutLevel), "/");

						previouslevelfile = fileMap.get(pPath);
						appendRecord(directory, ppathName, previouslevelfile);
					}
				}

				// Get the current path and the file where the entry should go
				for (int i = cutLevel - 1; i >= 0; i--) {
					if (i <= currentLevel % cutLevel) {
						if (i == 0) {
							pathName = pathName.concat(directory.getName());
							break;
						}

						final Directory node = levelDirectoryMap.get(currentLevel - i);
						pathName = pathName.concat(node.getName());
						pathName = pathName.concat("/");
					}
				}

				// Calculate the path
				final StringBuilder path = new StringBuilder();
				for (int n = 0; n <= currentLevel - currentLevel % cutLevel; n++) {
					final Directory node = levelDirectoryMap.get(n);
					path.append(node.getName());
					if (n == currentLevel - currentLevel % cutLevel) {
						break;
					}

					path.append("/");
				}

				// Write the entry to the file computed above
				levelfile = levelfile == null ? fileMap.get(path.toString()) : levelfile;
				if (currentLevel % cutLevel != 0) {
					appendRecord(directory, pathName, levelfile);
				}

				// Compute the key without the .txt extension
				String key = levelfile.getName().replace("+", "/");
				if (key.contains(".")) {
					key = key.substring(0, key.lastIndexOf('.'));
				}

				fileMap.put(key, levelfile);
			}
		}

		// After the file map has been constructed, get all files created and place them in DHT file system
		final File[] files = folder.listFiles();
		for (final File file : files) {
			fileSystem.copyFromLocal(file.getAbsolutePath(), file.getName());
		}

		// Return the map created
		return fileMap;
	}

	/**
	 * Appends the {@literal file} with parameters from {@link path} and {@link directory}
	 *
	 * @param directory
	 * @param path
	 * @param file
	 */
	private static void appendRecord(final Directory directory,
			final String path,
			final File file) {
		try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
			out.print(path);
			out.print("@" + directory.getAccessRights());
			out.print("@" + directory.getSize());
			out.print("@" + directory.getModifiedTimeStamp());
			out.println();
			out.close();
		} catch (final Exception e) {
			LOGGER.error("File not retreived", e);
		}
	}
}