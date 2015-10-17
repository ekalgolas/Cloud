package dht.hdfs.server.namenode;

import dht.dhtfs.core.Configuration;
import dht.nio.server.IProcessor;
import dht.nio.server.TCPServer;

public class NameNode {

	public static void main(String[] args) throws Exception {
		String confFile = "conf/masternode.conf";
		if (args.length >= 1) {
			confFile = args[0];
		}
		Configuration conf = new Configuration();
		conf.initialize(confFile);
		IProcessor processor = new NameNodeRequestProcessor();
		processor.initialize(conf);
		int port = Integer.parseInt(conf.getProperty("port"));
		TCPServer server = new TCPServer(port, processor);
		server.listen();
	}
}
