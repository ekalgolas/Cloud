package master.dht.dhtfs.server.datanode;

import master.dht.nio.server.IController;
import master.dht.nio.server.TCPServer;

import org.apache.log4j.PropertyConfigurator;

public class DataNode {

    public static void main(String[] args) throws Exception {
        String confFile = "conf/datanode.conf";
        if (args.length >= 1) {
            confFile = args[0];
        }
        DataServerConfiguration.initialize(confFile);
        PropertyConfigurator.configure(DataServerConfiguration
                .getLogConfigFile());
        IController controller = new DataRequestController();
        controller.initialize();
        int port = DataServerConfiguration.getPort();
        TCPServer server = new TCPServer(port, controller);
        server.listen();
    }

}
