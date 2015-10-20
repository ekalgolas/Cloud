package commons;

import java.io.File;
import java.io.IOException;
import java.util.InvalidPropertiesFormatException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Class to master.dht.test application configuration
 *
 * @author Ekal.Golas
 */
public class AppConfigTest {
	/**
	 * Temporary folder to create files for unit testing
	 */
	@Rule
	public final TemporaryFolder	folder	= new TemporaryFolder();

	/**
	 * Checks if application errors out if configuration directory is invalid
	 *
	 * @throws InvalidPropertiesFormatException
	 */
	@Test(expected = InvalidPropertiesFormatException.class)
	public void invalidLoadTest() throws InvalidPropertiesFormatException {
		new AppConfig("master.dht.test");
	}

	/**
	 * Test if null is return for an invalid key
	 */
	@Test
	public void invalidKeyTest() {
		Assert.assertNull("Did not return null for property that was absent", AppConfig.getValue("master.dht.test"));
	}

	/**
	 * Test if properties can be loaded successfully
	 * 
	 * @throws IOException
	 */
	@Test
	public void loadConfigurationTest()
			throws IOException {
		// Initialize
		final AppConfig appConfig = new AppConfig(folder.getRoot().getAbsolutePath());

		// Get a temporary file
		final File file = folder.newFile();

		// Write to file
		FileUtils.writeStringToFile(file, "master.dht.test=value");

		// Load and master.dht.test
		appConfig.loadConfiguration(file);
		Assert.assertEquals("Configuration not loaded as expected", "value", AppConfig.getValue("master.dht.test"));
	}
}