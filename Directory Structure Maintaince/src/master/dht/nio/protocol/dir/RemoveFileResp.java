package master.dht.nio.protocol.dir;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class RemoveFileResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    public RemoveFileResp(RespType responseType) {
        super(responseType);
    }

    public RemoveFileResp(int rId, RespType responseType) {
        super(rId, responseType);
    }
}
