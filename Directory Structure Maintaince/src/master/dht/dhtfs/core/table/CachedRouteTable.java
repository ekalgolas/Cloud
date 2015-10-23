package master.dht.dhtfs.core.table;

import java.io.IOException;
import java.util.List;

import master.dht.dhtfs.core.GeometryLocation;
import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.proxy.TableReq;
import master.dht.nio.protocol.proxy.TableResp;

public class CachedRouteTable {

    protected RouteTable table;
    protected PhysicalNode master;

    public CachedRouteTable(PhysicalNode master) {
        this.table = null;
        this.master = master;
    }

    public RouteTable updateRouteTable() throws IOException {
        TCPConnection connection = TCPConnection.getInstance(
                master.getIpAddress(), master.getPort());
        TableReq req = new TableReq(ReqType.TABLE);
        connection.request(req);
        TableResp resp = (TableResp) connection.response();
        table = resp.getTable();
        table.dump();
        connection.close();
        return table;
    }

    public boolean isAvailable(PhysicalNode local, String path) {
        return table.isAvailable(local, path);
    }

    public RouteTable getTable() {
        return table;
    }

    public PhysicalNode getNearestNode(String path, GeometryLocation location) {
        return table.getNearestNode(path, location);
    }

    public PhysicalNode getPrimary(String path) {
        return table.getPrimary(path);
    }

    public List<PhysicalNode> getPhysicalNodes(String path) {
        return table.getPhysicalNodes(path);
    }

}
