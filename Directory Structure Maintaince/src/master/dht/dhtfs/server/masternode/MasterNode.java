package master.dht.dhtfs.server.masternode;

import master.dht.nio.server.IController;
import master.dht.nio.server.TCPServer;

public class MasterNode {

    public static void main(String[] args) throws Exception {
        String confFile = "conf/masternode.conf";
        if (args.length >= 1) {
            confFile = args[0];
        }
        MasterConfiguration.initialize(confFile);
        IController controller = new MasterRequestController();
        controller.initialize();
        int port = MasterConfiguration.getPort();
        TCPServer server = new TCPServer(port, controller);
        server.listen();
        NodeMonitor monitor = new NodeMonitor(controller);
        new Thread(monitor).start();
    }

}
