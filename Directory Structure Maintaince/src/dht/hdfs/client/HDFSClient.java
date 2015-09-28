package dht.hdfs.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.def.IFile;
import dht.dhtfs.core.def.IFileSystem;

public class HDFSClient {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		String confFile = "conf/client.conf";
		if (args.length >= 1) {
			confFile = args[0];
		}
		Configuration conf = new Configuration();
		conf.initialize(confFile);
		IFileSystem dfs = new HDFSFileSystem();
		dfs.initialize(conf);
		// IDFSFile f = dfs.open(new AbsolutePath("/home/cyz0430/dht1.txt"));
		// dfs.create(new AbsolutePath("/cyz0430/dht1.txt"));
		// dfs.copyFromLocal(new DhtPath("/home/jemish/Files/Random/aaa.csv"),
		// new DhtPath("/a/b/c/d/f.txt"));
		// //
		dfs.create(new DhtPath("/a/b/b/s/h.txt"));
		// //

		long startTime = System.currentTimeMillis();
		System.out.println("Start time: " + startTime);
		HDFSFile ifile = (HDFSFile) dfs.open(new DhtPath("/a/b/b/s/h.txt"));
		long endTime = ifile.fileMeta.getTime();
		System.out.println("End Time: " + endTime);
		ifile.close();
		// long endTime = System.currentTimeMillis();
		//
		// long totalTime = endTime - startTime;
		//
		// System.out.println("Total time: " + totalTime);
		//
		// dfs.copyToLocal(new DhtPath("/a/b/c/d/f.txt"), new
		// DhtPath("/home/jemish/abc.txt"));

		// int lineNo = 0;
		// BufferedReader br = null;
		// try {
		// br = new BufferedReader(new FileReader(
		// "/home/jemish/FilesToTest/merge/merge-1-50.csv"));
		//
		// String line;
		// while ((line = br.readLine()) != null) {
		// // String[] split = line.split(",");
		//
		// try {
		//
		// IFile ifile = dfs.create(new DhtPath(line));
		// ifile.close();
		// System.out.println("lineNo: " + (++lineNo));
		//
		// // if ((lineNo % 25000) == 0) {
		// // Thread.sleep(52 * 1000);
		// // }
		//
		// } catch (Exception e) {
		//
		// // throw new Exception("problem in CSV file: " + split[0]);
		// e.printStackTrace();
		// }
		// }
		//
		// br.close();
		// } catch (FileNotFoundException e1) {
		// e1.printStackTrace();
		// } catch (IOException e) {
		// e.printStackTrace();
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		System.out.println("Done");

	}
}
