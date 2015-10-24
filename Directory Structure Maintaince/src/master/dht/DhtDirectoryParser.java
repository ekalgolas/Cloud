package master.dht;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import master.dht.dhtfs.client.ClientConfiguration;
import master.dht.dhtfs.client.DHTFileSystem;

import org.apache.commons.lang3.StringUtils;

import commons.dir.Directory;

/**
 * Parses DHT directory structure
 *
 * @author sahith
 */
public class DhtDirectoryParser {
	private static final int					ACCESS_RIGHT_ENDINDEX	= 10;
	private static final int					SIZE_STARTINDEX			= 10;
	private static HashMap<Integer, Directory>	levelDirectoryMap		= new HashMap<>();

	public static HashMap<String, File> parseText(final int cutLevel)
			throws IOException {
		// Initialize DHT
		ClientConfiguration.initialize("conf/dhtclient.conf");
		final DHTFileSystem fileSystem = new DHTFileSystem();
		fileSystem.initialize();

		final Directory directory = new Directory("root", false, new ArrayList<>());
		levelDirectoryMap.put(0, directory);

		final File root = File.createTempFile("root", ".txt");
		root.deleteOnExit();

		final HashMap<String, File> fileMap = new HashMap<>();
		fileMap.put("root", root);
		fileSystem.copyFromLocal(root.getAbsolutePath(), "root");

		try (final Scanner scanner = new Scanner(new File("/Users/sahith/Desktop/out.txt"))) {
			scanner.nextLine();
			while (scanner.hasNext()) {
				final String line = scanner.nextLine();
				if (StringUtils.isBlank(line)) {
					break;
				}

				int currentLevel = StringUtils.countMatches(line, "├") + StringUtils.countMatches(line, "└") + StringUtils.countMatches(line, "│");
				if (line.startsWith(" ")) {
					currentLevel++;
				}

				final String[] split = line.split("]");
				final String dirName = split[split.length - 1].trim();

				String name = dirName;
				boolean isFile = true;
				if (dirName.endsWith("/")) {
					name = dirName.substring(0, dirName.length() - 1);
					isFile = false;
				}

				final String details = split[0].substring(StringUtils.lastIndexOf(split[0], "[") + 1);
				final String accessRights = details.substring(0, ACCESS_RIGHT_ENDINDEX);
				final int sizeEndIndex = details.lastIndexOf(" ");
				final String readableTimeStamp = details.substring(sizeEndIndex).trim();
				final String size = details.substring(SIZE_STARTINDEX, sizeEndIndex).trim();

				final Directory dir = new Directory(name, isFile, new ArrayList<>(), Long.parseLong(readableTimeStamp), accessRights, Long.parseLong(size));
				levelDirectoryMap.put(currentLevel, dir);

				String pathName = "";
				File levelfile = null;
				File previouslevelfile = null;

				final Directory parent = levelDirectoryMap.get(currentLevel - 1);
				parent.getChildren().add(dir);

				String filename = "";
				if (currentLevel % cutLevel == 0) {
					for (int n = 0; n <= currentLevel; n++) {
						final Directory node = levelDirectoryMap.get(n);
						filename = filename.concat(node.getName());
						if (n == currentLevel) {
							break;
						}

						filename = filename.concat(":");
					}

					levelfile = File.createTempFile(filename, ".txt");
					levelfile.deleteOnExit();
					if (currentLevel > 0) {
						String ppathName = "";
						for (int b = currentLevel - cutLevel; b < currentLevel; b++) {
							final Directory node = levelDirectoryMap.get(b);
							ppathName = ppathName.concat(node.getName());
							ppathName = ppathName.concat("/");
						}

						ppathName = ppathName.concat(name);
						previouslevelfile = fileMap.get(levelDirectoryMap.get(currentLevel - cutLevel).getName());
						try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(previouslevelfile, true)))) {
							out.print(ppathName);
							out.print("@" + accessRights);
							out.print("@" + size);
							out.print("@" + readableTimeStamp);
							out.println();
							out.close();

						} catch (final Exception e) {
							System.out.println("file not retreived");
						}
					}
				}

				for (int i = cutLevel - 1; i >= 0; i--) {
					if (i <= currentLevel % cutLevel) {
						if (i == 0) {
							pathName = pathName.concat(name);
							break;
						}

						final Directory node = levelDirectoryMap.get(currentLevel - i);
						pathName = pathName.concat(node.getName());
						pathName = pathName.concat("/");
						if (i == currentLevel % cutLevel) {
							levelfile = fileMap.get(node.getName());
						}
					}

				}

				try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(levelfile, true)))) {
					out.print(pathName);
					out.print("@" + accessRights);
					out.print("@" + size);
					out.print("@" + readableTimeStamp);
					out.println();
					out.close();
				} catch (final Exception e) {
					System.out.println("not written to file");
				}

				fileMap.put(pathName, levelfile);
				fileSystem.copyFromLocal(levelfile.getAbsolutePath(), levelfile.getName());
			}
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		}

		return fileMap;
	}
}