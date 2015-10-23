package master.dht.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import master.dht.dhtfs.client.ClientConfiguration;
import master.dht.dhtfs.client.DHTFileSystem;

public class DFSClientCreate {

	public static void main(String[] args) {
		String csvFile = "/home/jemish/DirectoryPaths/20k_1";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			String confFile = "conf/client.conf";
			ClientConfiguration.initialize(confFile);
			DHTFileSystem dfs = new DHTFileSystem();
			dfs.initialize();
			// String src =
			// "/Users/yinzi_chen/Downloads/jdk-8u20-macosx-x64.dmg";
			// String dest = "a.dmg";

			// System.out.println("File created");

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				dfs.copyFromLocal("/home/jemish/Desktop/history", line);

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		System.out.println("Done");

	}

}
