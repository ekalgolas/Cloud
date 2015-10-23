package master.dht.nio.protocol.proxy;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class HeartBeatResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;
    private String uid;

    public HeartBeatResp(RespType responseType) {
        super(responseType);
    }

    public HeartBeatResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

}
