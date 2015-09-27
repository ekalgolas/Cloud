package dht.dhtfs.server.masternode;

import dht.dhtfs.core.Configuration;
import dht.nio.server.IProcessor;
import dht.nio.server.TCPServer;

public class MasterNode {

    public static void main(String[] args) throws Exception {
        String confFile = "conf/masternode.conf";
        if (args.length >= 1) {
            confFile = args[0];
        }
        Configuration conf = new Configuration();
        conf.initialize(confFile);
        IProcessor processor = new MasterRequestProcessor();
        processor.initialize(conf);
        int port = Integer.parseInt(conf.getProperty("port"));
        TCPServer server = new TCPServer(port, processor);
        server.listen();

    }

}
