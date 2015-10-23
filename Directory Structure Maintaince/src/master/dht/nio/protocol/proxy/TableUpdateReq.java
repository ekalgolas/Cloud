package master.dht.nio.protocol.proxy;

import master.dht.dhtfs.core.table.RouteTable.TableOperation;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class TableUpdateReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;
    private TableOperation op;

    public TableUpdateReq(ReqType requestType) {
        super(requestType);
    }

    public TableUpdateReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public TableOperation getOp() {
        return op;
    }

    public void setOp(TableOperation op) {
        this.op = op;
    }

}
