package dht.swift.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.def.IFile;
import dht.dhtfs.core.def.IFileSystem;

public class SwiftClient {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		String confFile = "conf/client.conf";
		if (args.length >= 1) {
			confFile = args[0];
		}
		Configuration conf = new Configuration();
		conf.initialize(confFile);
		IFileSystem swiftFS = new SwiftFileSystem();
		swiftFS.initialize(conf);

		// swiftFS.create(new DhtPath("/home/cyz0430/dht12.txt"));

		// swiftFS.open(new DhtPath("/home/cyz0430/dht12.txt"));

		int lineNo = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("/home/jemish/DirectoryPaths/LookUpFiles/HDFS/20k_1"));

			String line;
			while ((line = br.readLine()) != null) {
				// String[] split = line.split(",");

				try {
					// System.out.println("Request: " + split[0]);

					IFile iFile = swiftFS.create(new DhtPath(line));
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
