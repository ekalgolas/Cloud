package master.dht.test;

import java.io.IOException;

import master.dht.dhtfs.client.ClientConfiguration;
import master.dht.dhtfs.client.DHTFile;
import master.dht.dhtfs.client.DHTFileSystem;

public class DHTClient {

	public static void main(String[] args) throws IOException {
		String confFile = "conf/client.conf";
		if (args.length >= 1) {
			confFile = args[0];
		}
		ClientConfiguration.initialize(confFile);
		DHTFileSystem dfs = new DHTFileSystem();
		dfs.initialize();
		String src = "/home/jemish/Desktop/ge.deb";
		String dest = "/usr/share/abc/gnome-dev-memory.svg";
		// String src = "/Users/yinzi_chen/Downloads/jdk-8u20-macosx-x64.dmg";
		// String dest = "a.dmg";

		// dfs.create("/home/jemish/Desktop/Recordings/June/09 Jun.m4a");
		// System.out.println("File created");
		//
		// dfs.open("/home/jemish/Desktop/Recordings/June/09 Jun.m4a");
		// System.out.println("File opened");

		dfs.copyFromLocal(src, dest);
		System.out.println("upload succeed");
		// DHTFile file = dfs.open(dest);
		// byte[] b = new byte[20];
		// int len;

		// StringBuilder sb = new StringBuilder();
		// byte[] data = "hello world 6!\n".getBytes();
		// file.seek(0);
		// file.insert(data);
		// file.commit();
		// file.seek(0);
		// file.insert(data);
		// file.commit();
		// file.seek(0);
		// while ((len = file.read(b)) != -1) {
		// for (int i = 0; i < len; ++i) {
		// sb.append((char) b[i]);
		// }
		// }
		// System.out.println(sb.toString());
		// System.out.println(file.toString());
		// dfs.copyToLocal(dest, "b.txt");
	}
}
