package dht.dhtfs.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.def.IFile;
import dht.dhtfs.core.def.IFileSystem;

public class DHTClient {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		String confFile = "conf/client.conf";
		if (args.length >= 1) {
			confFile = args[0];
		}
		Configuration conf = new Configuration();
		conf.initialize(confFile);
		IFileSystem dfs = new DHTFileSystem();
		dfs.initialize(conf);
		// IDFSFile f = dfs.open(new AbsolutePath("/home/cyz0430/dht1.txt"));
		// dfs.create(new AbsolutePath("/cyz0430/dht1.txt"));
		// dfs.copyFromLocal(new DhtPath("/home/jemish/Files/Random/aaa.csv"),
		// new DhtPath("/a/b/c/d/f.txt"));
		// //
		// IFile iFile = dfs.create(new DhtPath("/a/b/b/s"));
		// iFile.close();
		// System.out.println("Done");
		// //
		// // dfs.open(new DhtPath("/a/b/b/s"));
		//
		// dfs.copyToLocal(new DhtPath("/a/b/c/d/f.txt"), new
		// DhtPath("/home/jemish/abc.txt"));

		int lineNo = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(
					"/home/jemish/FilesToTest/merge/merge-1-6.csv"));

			String line;
			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");

				try {
					// System.out.println("Request: " + split[0]);

					IFile iFile = dfs.create(new DhtPath(split[0]));
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
