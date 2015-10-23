package master.dht.nio.protocol.proxy;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class TableUpdateResp extends ProtocolResp {
    private static final long serialVersionUID = 1L;

    public TableUpdateResp(RespType responseType) {
        super(responseType);
    }

    public TableUpdateResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

}
