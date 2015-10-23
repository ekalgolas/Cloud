package master.dht;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;

import commons.dir.Directory;

public class DhtDirectoryParser {

	private static final int ACCESS_RIGHT_ENDINDEX = 10;
	private static final int SIZE_STARTINDEX = 10;
	private static HashMap<Integer, Directory> levelDirectoryMap = new HashMap<>();
	private static HashMap<String, File> fileMap = new HashMap<>();

	 public static HashMap<String, File> parsetext(int N) {

		final Directory directory = new Directory("root", false, new ArrayList<>());
		levelDirectoryMap.put(0, directory);
		File s = new File("/Users/sahith/Desktop/temp/root.txt");
		fileMap.put("root", s);

		try (final Scanner scanner = new Scanner(new File("/Users/sahith/Desktop/out.txt"))) {

			scanner.nextLine();

			while (scanner.hasNext()) {

				final String line = scanner.nextLine();
				if (StringUtils.isBlank(line)) {
					break;
				}

				int currentLevel = StringUtils.countMatches(line, "├") + StringUtils.countMatches(line, "└")
						+ StringUtils.countMatches(line, "│");
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

				String details = split[0].substring(StringUtils.lastIndexOf(split[0], "[") + 1);
				String accessRights = details.substring(0, ACCESS_RIGHT_ENDINDEX);
				int sizeEndIndex = details.lastIndexOf(" ");
				String readableTimeStamp = details.substring(sizeEndIndex).trim();
				String size = details.substring(SIZE_STARTINDEX, sizeEndIndex).trim();

				final Directory dir = new Directory(name, isFile, new ArrayList<>(), Long.parseLong(readableTimeStamp),
						accessRights, Long.parseLong(size));

				levelDirectoryMap.put(currentLevel, dir);

				String pathName = "";
				File levelfile = null;
				File previouslevelfile = null;

				final Directory parent = levelDirectoryMap.get(currentLevel - 1);
				parent.getChildren().add(dir);

				String filename = "";
				if (currentLevel % N == 0) {
					for (int n = 0; n <= currentLevel; n++) {
						final Directory node = levelDirectoryMap.get(n);
						filename = filename.concat(node.getName());
						if (n == currentLevel)
							break;
						filename = filename.concat(":");
					}

					levelfile = new File("/Users/sahith/Desktop/temp/" + filename + ".txt");

					if (currentLevel > 0) {
						String ppathName = "";

						for (int b = currentLevel - N; b < currentLevel; b++) {
							final Directory node = levelDirectoryMap.get(b);
							ppathName = ppathName.concat(node.getName());
							ppathName = ppathName.concat("/");
						}
						ppathName = ppathName.concat(name);
						previouslevelfile = fileMap.get(levelDirectoryMap.get(currentLevel - N).getName());

						try (PrintWriter out = new PrintWriter(
								new BufferedWriter(new FileWriter(previouslevelfile, true)))) {
							out.print(ppathName);
							out.print("@" + accessRights);
							out.print("@" + size);
							out.print("@" + readableTimeStamp);
							out.println();
							out.close();

						} catch (Exception e) {
							System.out.println("file not retreived");
						}

					}
				}

				for (int i = N - 1; i >= 0; i--) {

					if (i <= currentLevel % N) {

						if (i == 0) {
							pathName = pathName.concat(name);
							break;
						}
						final Directory node = levelDirectoryMap.get(currentLevel - i);
						pathName = pathName.concat(node.getName());
						pathName = pathName.concat("/");
						if (i == currentLevel % N) {
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

				} catch (Exception e) {
					System.out.println("not written to file");
				}

				fileMap.put(pathName, levelfile);

			}
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		}

		 return fileMap;
	}
//	 }

}
