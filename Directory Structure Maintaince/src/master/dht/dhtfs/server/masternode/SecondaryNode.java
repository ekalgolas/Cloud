package master.dht.dhtfs.server.masternode;

import master.dht.nio.server.IController;
import master.dht.nio.server.TCPServer;

public class SecondaryNode {

    public static void main(String[] args) throws Exception {
        String confFile = "conf/secondary.conf";
        if (args.length >= 1) {
            confFile = args[0];
        }
        SecondaryConfiguration.initialize(confFile);
        IController controller = new SecondaryRequestController();
        controller.initialize();
        int port = SecondaryConfiguration.getPort();
        TCPServer server = new TCPServer(port, controller);
        server.listen();
    }

}
