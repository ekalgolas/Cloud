package client;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import metadata.Directory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.distribution.ZipfDistribution;

import com.sun.media.sound.InvalidDataException;
import commons.AppConfig;
import commons.CommandsSupported;
import commons.dir.DirectoryParser;

/**
 * Class to generate commands using Zipf distribution
 *
 * @author Ekal.Golas
 */
public class CommandGenerator {
	/**
	 * Reads input file and generates an output file with each line having a command to the server
	 *
	 * @param size
	 *            Size of distribution
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void generateCommands(int size)
			throws UnsupportedEncodingException,
			FileNotFoundException,
			IOException {
		// Get list of commands supported
		final List<String> values = Stream.of(CommandsSupported.values())
				.map(CommandsSupported::name)
				.filter(x -> x != CommandsSupported.EXIT.name())
				.collect(Collectors.toList());
		String[] commands = values.toArray(new String[values.size()]);

		// Get paths
		String[] paths = getAllPaths(DirectoryParser.parseText(AppConfig.getValue("client.inputFile")));

		// Distribute paths and commands
		paths = createZipfDistribution(paths);
		commands = createZipfDistribution(commands);

		// Write these commands to a file
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(AppConfig.getValue("client.commandsFile")), "utf-8"))) {
			final Random random = new Random();
			while (size-- > 0) {
				// Get random path and command
				final String path = paths[random.nextInt(paths.length)];
				final String command = commands[random.nextInt(commands.length)];

				// Write the line obtained
				writer.write(command + " " + path + "\n");
			}

			// Lastly write the exit command
			writer.write(CommandsSupported.EXIT.name());
		}
	}

	/**
	 * <pre>
	 * Gets all possible paths in a directory structure
	 * NOTE: default for unit testing
	 * </pre>
	 *
	 * @param root
	 *            Root of directory structure
	 * @return Array of paths as strings
	 * @throws InvalidDataException
	 */
	String[] getAllPaths(final Directory root)
			throws InvalidDataException {
		// Create a list
		final Set<String> paths = new HashSet<>();

		// Check if metadata root exists
		if (root == null) {
			throw new InvalidDataException("Metadata root not initialized, cannot create commands");
		}

		// Traverse the tree to get the paths
		traverse(root, paths, new ArrayList<>(), 0);

		// Return the array derived from the list
		return paths.toArray(new String[paths.size()]);
	}

	/**
	 * <pre>
	 * Traverse and get all paths in a tree
	 * NOTE: default for unit testing
	 * </pre>
	 *
	 * @param root
	 *            Root of the tree
	 * @param paths
	 *            List of paths to add the paths to
	 * @param path
	 *            Variable to keep track of current path
	 * @param index
	 *            Index of the current node
	 */
	void traverse(final Directory root,
			final Set<String> paths,
			final List<String> path,
			int index) {
		// If root is null, just exit
		if (root == null) {
			return;
		}

		// Add this node to the current path
		if (path.size() > index) {
			path.set(index, root.getName());
		} else {
			path.add(root.getName());
		}

		// Increment index for the next child
		index++;

		// Add this path till the index to the list of paths
		paths.add(StringUtils.join(path.subList(0, index), "/"));

		// Process all the children of this node
		for (final Directory child : root.getChildren()) {
			traverse(child, paths, path, index);
		}
	}

	/**
	 * <pre>
	 * Takes in a string array and returns the same array in which elements are distributed according to zipf distribution
	 * NOTE: default for unit testing
	 * </pre>
	 *
	 * @param collection
	 *            Array to randomize
	 * @return Weighted distribution of the array
	 */
	String[] createZipfDistribution(final String[] collection) {
		// Create an array for distribution
		final String[] distribution = new String[collection.length];

		// Initialize a zipf distribution
		final ZipfDistribution zipfDistribution = new ZipfDistribution(collection.length, 1);
		for (int i = 0; i < collection.length; i++) {
			// Take a sample from the distribution
			final int random = zipfDistribution.sample();

			// Select the sample index from the collection and assign it to current index of distributed array
			distribution[i] = collection[random - 1];
		}

		// Return the distribution
		return distribution;
	}
}