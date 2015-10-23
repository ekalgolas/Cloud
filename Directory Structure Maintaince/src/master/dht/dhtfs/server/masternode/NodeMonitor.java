package master.dht.dhtfs.server.masternode;

import java.io.IOException;
import java.util.List;

import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.dhtfs.core.table.RouteTable;
import master.dht.nio.client.TCPClient;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.proxy.HeartBeatReq;
import master.dht.nio.server.IController;

public class NodeMonitor implements Runnable {

    TCPClient client;
    RouteTable table;

    public NodeMonitor(IController controller) throws IOException {
        table = RouteTable.getInstance();
        client = new TCPClient(controller);
        new Thread(client).start();
    }

    @Override
    public void run() {
        int heartBeatPeriod = MasterConfiguration.getHeartBeatPeriod();
        while (true) {
            List<PhysicalNode> nodes = table.getOnlineServers();
            if (nodes.isEmpty()) {
                try {
                    Thread.sleep(heartBeatPeriod);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                continue;
            }
            int interval = heartBeatPeriod / nodes.size();
            for (PhysicalNode node : nodes) {
                HeartBeatReq req = new HeartBeatReq(ReqType.HEART_BEAT);
                try {
                    client.register(node.getUid(), node.getIpAddress(),
                            node.getPort());
                    client.addReq(node.getUid(), req);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(interval);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
