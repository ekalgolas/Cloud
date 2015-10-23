package client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.sun.media.sound.InvalidDataException;

import commons.AppConfig;
import commons.dir.Directory;

/**
 * Class to master.dht.test command generation
 *
 * @author Ekal.Golas
 */
public class CommandGeneratorTest {
	private final CommandGenerator	generator	= new CommandGenerator();

	/**
	 * Temporary folder to create files for unit testing
	 */
	@Rule
	public final TemporaryFolder	folder		= new TemporaryFolder();

	/**
	 * Test if an invalid directory creates error or not
	 *
	 * @throws InvalidDataException
	 */
	@Test(expected = InvalidDataException.class)
	public void testInvalidDirectory() throws InvalidDataException {
		generator.getAllPaths(null);
	}

	/**
	 * Test if we get empty result when directory is null
	 */
	@Test
	public void testInvalidTraverse() {
		// Call traverse with null directory
		final Set<String> paths = new HashSet<>();
		generator.traverse(null, paths, null, 0);
		assertEquals("Expected no paths in the result but got some", 0, paths.size());
	}

	/**
	 * Test if we can get all the directory paths in a directory tree correctly
	 *
	 * @throws InvalidDataException
	 */
	@Test
	public void testTraverse() throws InvalidDataException {
		// Create a directory structure
		final Directory rootNode = new Directory("root", false, new ArrayList<Directory>());
		final Directory node1 = new Directory("1", false, new ArrayList<Directory>());
		final Directory node2 = new Directory("2", false, new ArrayList<Directory>());
		final Directory node3 = new Directory("3", false, new ArrayList<Directory>());
		final Directory node4 = new Directory("4", false, new ArrayList<Directory>());
		final Directory node5 = new Directory("5", false, new ArrayList<Directory>());
		final Directory node6 = new Directory("6", false, new ArrayList<Directory>());
		final Directory node7 = new Directory("7", false, new ArrayList<Directory>());
		final Directory node8 = new Directory("8", false, new ArrayList<Directory>());

		rootNode.getChildren().add(node2);
		rootNode.getChildren().add(node6);
		node2.getChildren().add(node1);
		node2.getChildren().add(node3);
		node6.getChildren().add(node5);
		node6.getChildren().add(node7);
		node1.getChildren().add(node4);
		node5.getChildren().add(node8);

		// Get paths
		final Set<String> paths = new HashSet<>();
		generator.traverse(rootNode, paths, new ArrayList<>(), 0);

		// Test if there are 9 unique paths
		assertEquals("Could not get all paths correctly", 9, paths.size());

		// Call get all paths
		final String[] result = generator.getAllPaths(rootNode);
		assertEquals("Could not get list of paths correctly", 9, result.length);
	}

	/**
	 * Test creation of a zipf distribution
	 */
	@Test
	public void testZipfDistribution() {
		// Create a string array
		final String[] array = new String[10];
		for (int i = 0; i < array.length; i++) {
			array[i] = String.valueOf(i);
		}

		// Create zipf distribution
		final String[] dist = generator.createZipfDistribution(array);

		// Test for weighted randomness
		final Set<String> set = new HashSet<>();
		for (final String string : dist) {
			// Test if values are in range
			if (Integer.parseInt(string) < 0 || Integer.parseInt(string) > 9) {
				fail("Values generated out of range");
			}

			set.add(string);
		}

		// Test if set contains repeated elements as it is weighted distribution
		assertTrue("Number of unique elements in weighted distribution should be lesser than the original collection", set.size() < dist.length);
	}

	/**
	 * Tests if commands can be generated successfully from an input file
	 *
	 * @throws IOException
	 */
	@Test
	public void testCommandGeneration()
			throws IOException {
		// Create files for configuration, input and output
		final File conf = folder.newFile();
		final File inputFile = folder.newFile();
		final File outputFile = folder.newFile();

		// Write configuration data
		FileUtils.writeStringToFile(conf, "client.inputFile=" + inputFile.getPath().replace("\\", "/") + "\n");
		FileUtils.writeStringToFile(conf, "client.commandsFile=" + outputFile.getPath().replace("\\", "/"), true);

		// Write master.dht.test data in input file
		FileUtils.writeStringToFile(inputFile, "Fall 2015\n" + "├── [-rw-rw-r--      198852 1443563302]  AcademicCalendarFall2015.pdf\n"
				+ "├── [drwxrwxr-x    22161867 1443921137]  AOS/\n" + "│   ├── [-rw-rw-r--        3016 1443670856]  grade.sh\n"
				+ "│   ├── [-rw-rw-r--         344 1443670856]  grading-output.txt\n"
				+ "│   ├── [-rw-rw-rw-         625 1442265401]  launcher.sh\n" + "│   ├── [drwxrwxr-x     9313786 1443629435]  Lectures/\n"
				+ "│   │   ├── [-rw-rw-r--      627887 1443629410]  commit.pptx\n"
				+ "│   │   ├── [-rw-rw-r--     1468140 1441076550]  Commit Protocols.pdf\n"
				+ "│   │   ├── [-rw-rw-r--     2712828 1441076532]  foundations-animated.pdf\n"
				+ "│   │   ├── [-rw-rw-r--      233027 1441076542]  foundations-nonanimated.pdf\n"
				+ "│   │   ├── [-rw-rw-r--      140545 1443629435]  mutex-handout(1).pdf\n"
				+ "│   │   └── [-rw-rw-r--     4127263 1443629399]  mutex-slides.pdf\n"
				+ "│   ├── [-rw-rw-r--     8305617 1442890880]  New Doc 2.pdf");

		// Create configuration and call command generation
		new AppConfig(folder.getRoot().getAbsolutePath());
		generator.generateCommands(10);

		// Read the commands and see if size matches
		final List<String> commands = FileUtils.readLines(outputFile);
		assertEquals("Expected number of commands created are not equal to the commands created", 11, commands.size());

		// Also check if last line is exit
		assertEquals("Last line is not exit", "EXIT", commands.get(10));
	}
}