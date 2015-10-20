package master.dht.dhtfs.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import master.dht.dhtfs.core.def.IFile;
import master.dht.dhtfs.core.def.IFileSystem;

public class DHTClient {

	public static void main(String[] args) throws IOException {
		String confFile = "conf/client.conf";
		if (args.length >= 1) {
			confFile = args[0];
		}
		ClientConfiguration.initialize(confFile);
		DHTFileSystem dfs = new DHTFileSystem();
		dfs.initialize();
		String src = "/home/jemish/Desktop/Recordings/June/09 Jun.m4a";
		String dest = "/cyz0430/b.dmg";
		// String src = "/Users/yinzi_chen/Downloads/jdk-8u20-macosx-x64.dmg";
		// String dest = "a.dmg";

		// dfs.create("/home/jemish/Desktop/Recordings/June/09 Jun.m4a");
		// System.out.println("File created");
		//
		// dfs.open("/home/jemish/Desktop/Recordings/June/09 Jun.m4a");
		// System.out.println("File opened");

		int lineNo = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(
					"/home/jemish/DirectoryPaths/LookUpFiles/HDFS/20k_2"));

			String line;
			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");

				try {
					// System.out.println("Request: " + split[0]);

					IFile iFile = dfs.create(split[0]);
					iFile.close();
					System.out.println("lineNo: " + (++lineNo));

					if ((lineNo % 25000) == 0) {
						Thread.sleep(61 * 1000);
					}
				} catch (Exception e) {

					// throw new Exception("problem in CSV file: " + split[0]);
					e.printStackTrace();
				}
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Done");

	}
}
