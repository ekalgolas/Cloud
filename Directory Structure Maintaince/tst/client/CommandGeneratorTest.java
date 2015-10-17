package client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import metadata.Directory;

import org.junit.Test;

import com.sun.media.sound.InvalidDataException;

/**
 * Class to test command generation
 *
 * @author Ekal.Golas
 */
public class CommandGeneratorTest {
	private final CommandGenerator	generator	= new CommandGenerator();

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
}