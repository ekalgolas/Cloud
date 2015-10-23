package master.dht.nio.protocol.proxy;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class HeartBeatReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;

    public HeartBeatReq(ReqType requestType) {
        super(requestType);
    }

    public HeartBeatReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

}
