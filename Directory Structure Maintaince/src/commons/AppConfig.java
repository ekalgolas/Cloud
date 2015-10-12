package commons;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

/**
 * Class to load application configuration
 *
 * @author Ekal.Golas
 */
public class AppConfig {
	private static Properties	properties	= new Properties();

	/**
	 * Constructor
	 *
	 * @param path
	 *            Configuration directory path
	 * @throws InvalidPropertiesFormatException
	 */
	public AppConfig(final String path) throws InvalidPropertiesFormatException {
		// Get all files in configuration directory
		final File directory = new File(path);
		if (directory.isDirectory()) {
			// For each file, load its configuration
			for (final File file : directory.listFiles()) {
				if (file.isFile()) {
					loadConfiguration(file);
				}
			}
		} else {
			// Else, error out if directory is not parsed
			throw new InvalidPropertiesFormatException("Configuration directory not parsed");
		}
	}

	/**
	 * <pre>
	 * Loads configuration in a file
	 * NOTE: Default for unit testing
	 * </pre>
	 *
	 * @param file
	 *            File to load configuration from
	 */
	void loadConfiguration(final File file) {
		final Properties prop = new Properties();

		try {
			// Load properties and put them in global properties
			prop.load(new FileInputStream(file));
			properties.putAll(prop);
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get the value related with the key
	 *
	 * @param key
	 *            Key name for the property
	 * @return Value as string, null if key is not found
	 */
	public static String getValue(final String key) {
		return properties.getProperty(key);
	}
}