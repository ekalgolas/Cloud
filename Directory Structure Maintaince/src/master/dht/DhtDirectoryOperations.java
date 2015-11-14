package master.dht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import master.dht.dhtfs.client.ClientConfiguration;
import master.dht.dhtfs.client.DHTFileSystem;

import org.apache.log4j.Logger;

import com.google.common.io.Files;
import com.sun.media.sound.InvalidDataException;

import commons.AppConfig;
import commons.Globals;
import commons.Message;
import commons.OutputFormatter;
import commons.dir.Directory;
import commons.dir.ICommandOperations;

public class DhtDirectoryOperations implements ICommandOperations {
	private static final int		CUT_LEVEL	= Integer.parseInt(AppConfig.getValue("server.dht.cutLevel"));
	private final static Logger		LOGGER		= Logger.getLogger(DhtDirectoryOperations.class);
	private static DHTFileSystem	fileSystem;
	private static File				folder;

	/**
	 * Constructor
	 *
	 * @throws IOException
	 */
	public DhtDirectoryOperations() throws IOException {
		// Initialize DHT
		ClientConfiguration.initialize("conf/dhtclient.conf");
		fileSystem = new DHTFileSystem();
		fileSystem.initialize();

		// Get a temporary folder
		folder = Files.createTempDir();
		folder.deleteOnExit();
	}

	/* (non-Javadoc)
	 * @see commons.dir.ICommandOperations#ls(commons.dir.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public Message ls(final Directory root,
			String filePath,
			final String... arguments)
					throws InvalidPropertiesFormatException,
					InvalidDataException {
		// Replace the spaces in path
		filePath = filePath.replace(" ", "-");

		String line;
		final StringBuilder pathname = new StringBuilder();
		final String[] names = filePath.split("/");
		final String path = getPath(filePath, pathname, names, names.length - 1);

		final File file = getFileFromDHT(filePath, path);
		final Message builder = new Message("Listing for " + filePath + "\n");
		final OutputFormatter output = new OutputFormatter();
		output.addRow("TYPE", "NAME");

		String filenameduplicate = "";
		final String name = names[names.length - 1];
		try {
			// Read the file
			final BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
			while ((line = bufferedReader.readLine()) != null) {
				// For each line that contains the pathname
				if (line.contains(pathname)) {
					final String[] split = line.split("@");
					String filename;

					// Get the child after the pathname
					final String child = split[0].substring(split[0].lastIndexOf(name) + name.length())
							.trim();
					if (child == null || child.equals("")) {
						// If child is null, it means it is a file
						continue;
					} else if (child.equals("/")) {
						// If just "/"
						filename = child;
					} else if (child.contains("/")) {
						// If a directory get the next child
						filename = child.split("/")[1];
					} else {
						// Else, just get the file
						filename = child;
					}

					if (filenameduplicate.equalsIgnoreCase(filename)) {
						continue;
					}

					// Figure out if child is a file or directory and write output
					filenameduplicate = filename;
					if (line.contains("@-rw")) {
						output.addRow("File", filename);
					} else {
						output.addRow("Directory", filename);
					}
				}
			}

			// Error on path as a file and if directory is empty
			bufferedReader.close();
			if (filenameduplicate.equalsIgnoreCase("")) {
				throw new InvalidDataException(filePath + " is a file. Expecting directory!");
			}
			if (filenameduplicate.equalsIgnoreCase("/")) {
				throw new InvalidDataException("Directory is empty");
			}
		} catch (final Exception e) {
			throw new InvalidDataException(e.getMessage());
		}

		builder.appendContent(output.toString());
		return builder;
	}

	public static void mkdir(final HashMap<String, File> filemap,
			final String path)
					throws InvalidPropertiesFormatException {
		final Directory directory = new Directory("", false, null, System.currentTimeMillis(), (long) 4096);
		File file = null;
		final List<String> names = new ArrayList<String>();
		final String filePath = getFilePath(path, names);

		String pathname = "";
		if (names.size() - 1 <= CUT_LEVEL) {
			file = filemap.get(filePath);
			pathname = filePath;
		} else {
			for (int i = CUT_LEVEL - 1; i >= 0; i--) {
				if (i <= (names.size() - 2) % CUT_LEVEL) {
					if (i == 0) {
						pathname = pathname.concat(names.get(names.size() - 2));
						break;
					}

					pathname = pathname.concat(names.get(names.size() - i - 2));
					pathname = pathname.concat("/");
				}
			}

			file = filemap.get(pathname);
		}

		if (file == null) {
			throw new InvalidPathException(filePath, "Does not exist");
		}

		try {
			file = new File(folder.getAbsolutePath() + "/" + file.getName());
			fileSystem.copyToLocal(file.getName(), file.getAbsolutePath());
		} catch (final IOException e) {
			e.printStackTrace();
		}

		final String regexshort = getRegex();
		String filename = "";
		File levelfile = null;
		if (filePath.matches(regexshort)) {
			for (int l = 0; l < names.size(); l++) {
				if (l == names.size() - 1) {
					filename = filename.concat(names.get(l));
					break;
				}

				filename = filename.concat(names.get(l));
				filename = filename.concat("+");
			}

			levelfile = new File(folder.getAbsolutePath() + "/" + filename + ".txt");
			appendRecord(directory, names.get(names.size() - 1), levelfile);
			filemap.put(pathname + "/" + names.get(names.size() - 1), levelfile);
		}

		appendRecord(directory, pathname + "/" + names.get(names.size() - 1), file);
		try {
			fileSystem.copyFromLocal(levelfile.getAbsolutePath(), levelfile.getName());
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see commons.dir.ICommandOperations#touch(commons.dir.Directory, java.lang.String, java.lang.String[])
	 */
	@Override
	public Message touch(final Directory root,
			String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		// Replace the spaces
		path = path.replace(" ", "-");

		final StringBuilder pathname = new StringBuilder();
		String[] names = new String[100];
		String filePath = "";
		if (path.contains("/")) {
			names = path.split("/");
			for (int q = 0; q < names.length - 1; q++) {
				if (q == names.length - 2) {
					filePath = filePath.concat(names[q]);
					break;
				}

				filePath = filePath.concat(names[q]);
				filePath = filePath.concat("/");
			}
		} else {
			names[0] = path;
			filePath = path;
		}

		final String dhtPath = getPath(filePath, pathname, names, names.length - 2);
		File file;
		try {
			file = getFileFromDHT(path, dhtPath);
		} catch (final InvalidDataException e) {
			throw new InvalidPropertiesFormatException(e);
		}

		PrintWriter writer;
		BufferedReader reader;
		int counter = 0, counter1 = 0;
		String line = "", currentLine = "";
		final File temp = new File(file.getAbsolutePath() + ".tmp");
		try {
			reader = new BufferedReader(new FileReader(file));
			writer = new PrintWriter(new BufferedWriter(new FileWriter(temp)));
			while ((currentLine = reader.readLine()) != null) {
				String trimmedLine = currentLine.trim();
				if (trimmedLine.contains(pathname)) {
					if (counter == 0) {
						line = trimmedLine;
						counter++;
					}

					if (trimmedLine.contains(pathname + "/" + names[names.length - 1])) {
						counter1++;
						final String[] split = trimmedLine.split("@");
						trimmedLine = "";
						split[3] = Long.toString(System.currentTimeMillis());

						for (int b = 0; b <= 3; b++) {
							if (b == 3) {
								trimmedLine = trimmedLine.concat(split[b]);
								break;
							}

							trimmedLine = trimmedLine.concat(split[b]);
							trimmedLine = trimmedLine.concat("@");
						}
					}
				}

				writer.print(trimmedLine);
				writer.println();
				writer.flush();
			}
			if (counter1 == 0) {
				final String[] split = line.split("@");
				line = "";
				if (split[1].contains("d")) {
					split[0] = split[0].concat("/" + names[names.length - 1]);
					split[1] = "-rw-r--r--";
					split[2] = "0";
					for (int c = 0; c <= 3; c++) {
						if (c == 3) {
							line = line.concat(split[c]);
							break;
						}

						line = line.concat(split[c]);
						line = line.concat("@");
					}

					writer.print(line);
					writer.println();
					writer.close();
				} else {
					writer.close();
					reader.close();
					throw new InvalidPathException(filePath, "cannot create a file inside a file");
				}
			}

			reader.close();
		} catch (final IOException e) {
			throw new InvalidPropertiesFormatException("Failed to modify metadata in local");
		}

		try {
			fileSystem.delete(file.getName());
			fileSystem.copyFromLocal(temp.getAbsolutePath(), file.getName());
		} catch (final IOException e) {
			throw new InvalidPropertiesFormatException("Failed to update metadata in DHT");
		}

		return new Message("Touch Successful");
	}

	public static void rmdirf(final HashMap<String, File> filemap,
			final String filePath)
					throws InvalidPropertiesFormatException {
		String pathname = "";
		File f = null;
		String[] names = new String[CUT_LEVEL];

		if (filePath.contains("/")) {
			names = filePath.split("/");
		} else {
			names[0] = filePath;
		}

		if (names.length <= CUT_LEVEL) {
			f = filemap.get(filePath);
			pathname = filePath;
		}

		else {

			for (int i = CUT_LEVEL - 1; i >= 0; i--) {
				if (i <= (names.length - 1) % CUT_LEVEL) {
					if (i == 0) {
						pathname = pathname.concat(names[names.length - 1]);
						break;
					}
					pathname = pathname.concat(names[names.length - i - 1]);
					pathname = pathname.concat("/");

				}
			}
			f = filemap.get(pathname);
		}

		if (f == null) {
			throw new InvalidPathException(filePath, "Does not exist");
		}

		BufferedReader reader;
		PrintWriter writer;
		String currentLine;
		final String[] fnames = new String[1000];
		final File temp = new File(f.getAbsolutePath() + ".tmp");
		int p = 0;

		try {
			reader = new BufferedReader(new FileReader(f));
			writer = new PrintWriter(new BufferedWriter(new FileWriter(temp)));

			while ((currentLine = reader.readLine()) != null) {
				final String trimmedLine = currentLine.trim();

				if (trimmedLine.contains(pathname)) {
					final String[] split = trimmedLine.split("@");

					if (split[0].matches(".*/.*/.*/.*")) {
						final String[] name = split[0].split("/");
						fnames[p] = name[CUT_LEVEL];
						p++;

					}

					continue;
				}
				writer.println(currentLine);
			}

			f.delete();
			writer.close();
			reader.close();
			final boolean successful = temp.renameTo(f);
			if (successful == false) {
				System.out.println("not succesfully renamed");
			}

			for (int g = 0; g < p; g++) {

				System.out.println(g + ": " + fnames[g]);
				if (g == 64) {
					System.out.println(g);
				}

				rmdirf(filemap, fnames[g]);
			}
		} catch (final IOException e) {
			System.out.println("execption");
		}

	}

	@Override
	public Message mkdir(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		mkdir(Globals.dhtFileMap, path);

		final Message message = new Message("Directory successfully created");
		return message;
	}

	@Override
	public Message rmdir(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		rmdirf(Globals.dhtFileMap, path);
		return new Message("rmdir Successful");
	}

	@Override
	public void rm(final Directory root,
			final String path)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public Message cd(final Directory root,
			final String filePath)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Gets a file from DHT
	 *
	 * @param filePath
	 *            Path of the file in local
	 * @param path
	 *            Path in DHT
	 * @return File object copied in local
	 * @throws InvalidDataException
	 */
	private File getFileFromDHT(final String filePath,
			final String path)
					throws InvalidDataException {
		// Get the file from DHT, throw error if not found
		try {
			fileSystem.copyToLocal(path, folder.getAbsolutePath() + "/" + path);
		} catch (final IOException e) {
			throw new InvalidDataException(filePath + " does not exist!");
		}

		// Read this file and construct output in a table format
		final File file = new File(folder.getAbsolutePath() + "/" + path);
		return file;
	}

	/**
	 * Gets path in the DHT
	 *
	 * @param filePath
	 *            File path in local
	 * @param pathname
	 *            String builder to calculate path
	 * @param names
	 *            Array of directories in the path
	 * @param index
	 *            Index in the names array
	 * @return Path as string
	 */
	private String getPath(final String filePath,
			final StringBuilder pathname,
			final String[] names,
			final int index) {
		String path;
		if (names.length <= CUT_LEVEL) {
			pathname.append("root");
			path = pathname.toString() + ".txt";
		} else {
			for (int i = CUT_LEVEL - 1; i >= 0; i--) {
				if (i <= index % CUT_LEVEL) {
					if (i == 0) {
						pathname.append(names[index]);
						break;
					}

					pathname.append(names[index - i]);
					pathname.append("/");
				}
			}

			path = pathname.toString().split("/")[0];
			final String key = filePath.substring(0, filePath.length() - pathname.length());
			path = (key + path + ".txt").replace("/", "+");
		}
		return path;
	}

	/**
	 * Computes a regex to match the file paths
	 *
	 * @return Regex as string
	 */
	private static String getRegex() {
		String regexshort = "";
		for (int s = 0; s < CUT_LEVEL; s++) {
			if (s == CUT_LEVEL - 1) {
				regexshort = regexshort.concat(".*");
				break;
			}

			regexshort = regexshort.concat(".*/");
		}

		return regexshort;
	}

	/**
	 * Takes in a path and gets the file path
	 *
	 * @param path
	 *            Path as string
	 * @param names
	 *            Array of path names
	 * @return File path
	 */
	private static String getFilePath(final String path,
			final List<String> names) {
		String filePath = "";
		if (path.contains("/")) {
			names.addAll(Arrays.asList(path.split("/")));
			for (int q = 0; q < names.size() - 1; q++) {
				if (q == names.size() - 2) {
					filePath = filePath.concat(names.get(q));
					break;
				}

				filePath = filePath.concat(names.get(q));
				filePath = filePath.concat("/");
			}
		} else {
			names.add(path);
			filePath = path;
		}

		return filePath;
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
			out.print("@" + directory.getSize());
			out.print("@" + directory.getModifiedTimeStamp());
			out.println();
			out.close();
		} catch (final Exception e) {
			LOGGER.error("File not retreived", e);
		}
	}

    @Override
    public Directory releaseParentReadLocks(Directory root, String filePath) {
        // TODO Auto-generated method stub
        return null;
    }
}