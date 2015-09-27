package dht.test;

import java.io.IOException;

import dht.dhtfs.client.DHTFileSystem;
import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;

public class DHTClient {

	public static void main(String[] args) throws IOException {
		String confFile = "conf/client.conf";
		if (args.length >= 1) {
			confFile = args[0];
		}
		Configuration conf = new Configuration();
		conf.initialize(confFile);
		DHTFileSystem dfs = new DHTFileSystem();
		dfs.initialize(conf);
		// IDFSFile f = dfs.open(new AbsolutePath("/home/cyz0430/dht1.txt"));
		// dfs.create(new AbsolutePath("/cyz0430/dht1.txt"));
		dfs.copyFromLocal(new DhtPath("/Users/yinzi_chen/Downloads/jdk-8u20-macosx-x64.dmg"),
				new DhtPath("/cyz0430/jdk-8u20-macosx-x64.dmg"));
	}
}
