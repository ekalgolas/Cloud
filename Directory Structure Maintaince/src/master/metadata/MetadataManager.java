package master.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import master.dht.DhtDirectoryParser;

import org.apache.log4j.Logger;

import commons.AppConfig;
import commons.dir.Directory;
import commons.dir.DirectoryParser;

/**
 * Class that manages the directory metadata
 *
 * @author Ekal.Golas
 */
public class MetadataManager {
	private final static Logger	LOGGER	= Logger.getLogger(MetadataManager.class);

	/**
	 * Create in-memory metadata structure for GFS
	 *
	 * @return Root of directory structure
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Directory generateGFSMetadata()
			throws IOException,
			ClassNotFoundException {
		final File metadataStore = new File(AppConfig.getValue("server.gfs.metadataFile"));
		Directory directory = null;
		if (metadataStore.exists()) {
			// Read the file into an object
			try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(metadataStore))) {
				directory = (Directory) inputStream.readObject();
			}
		} else {
			// Parse the directory
			directory = DirectoryParser.parseText(AppConfig.getValue("server.inputFile"));

			// Serialize and store the master.metadata
			storeGFSMetadata(directory);
		}

		return directory;
	}

	/**
	 * Create in-memory metadata structure for DHT
	 *
	 * @return Map of string to file
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public static HashMap<String, File> generateDHTMetadata()
			throws IOException,
			ClassNotFoundException {
		final File metadataStore = new File(AppConfig.getValue("server.dht.metadataFile"));
		HashMap<String, File> fileMap = null;
		if (metadataStore.exists()) {
			// Read the file into an object
			try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(metadataStore))) {
				fileMap = (HashMap<String, File>) inputStream.readObject();
			}
		} else {
			// Parse the directory
			fileMap = DhtDirectoryParser.parseText(Integer.parseInt(AppConfig.getValue("server.dht.cutLevel")));

			// Serialize and store the master.metadata
			storeDHTMetadata(fileMap);
		}

		return fileMap;
	}

	/**
	 * Serialize the metadata and write it into the file
	 *
	 * @param directory
	 *            {@link Directory} object to be written
	 */
	public static void storeGFSMetadata(final Directory directory) {
		final File metadataStore = new File(AppConfig.getValue("server.gfs.metadataFile"));
		try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(metadataStore))) {
			outputStream.writeObject(directory);
		} catch (final IOException e) {
			LOGGER.error("", e);
		}
	}

	/**
	 * Serialize the metadata and write it into the file
	 *
	 * @param fileMap
	 *            {@link Directory} object to be written
	 */
	public static void storeDHTMetadata(final HashMap<String, File> fileMap) {
		final File metadataStore = new File(AppConfig.getValue("server.dht.metadataFile"));
		try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(metadataStore))) {
			outputStream.writeObject(fileMap);
		} catch (final IOException e) {
			LOGGER.error("", e);
		}
	}
}