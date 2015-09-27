package dht.dhtfs.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Configuration {

	protected Properties properties;

	public Configuration() {
		properties = new Properties();
	}

	public void initialize(String propertiesFile) throws IOException {
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(propertiesFile);
		} catch (FileNotFoundException e) {
			throw new IOException(e.getMessage(), e);
		}
		properties.load(fis);
	}

	public String getProperty(String name) {
		return properties.getProperty(name);
	}
}
