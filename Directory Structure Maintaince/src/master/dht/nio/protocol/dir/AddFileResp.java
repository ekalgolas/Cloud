package master.dht.nio.protocol.dir;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class AddFileResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    public AddFileResp(RespType responseType) {
        super(responseType);
    }

    public AddFileResp(int rId, RespType responseType) {
        super(rId, responseType);
    }
}
