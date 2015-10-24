package master.dht.nio.protocol.proxy;

import master.dht.dhtfs.core.table.RouteTable;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class TableResp extends ProtocolResp {
    private static final long serialVersionUID = 1L;

    private RouteTable table;

    public TableResp(RespType responseType) {
        super(responseType);
    }

    public TableResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public RouteTable getTable() {
        return table;
    }

    public void setTable(RouteTable table) {
        this.table = table;
    }

}