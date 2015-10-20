package master.dht.nio.protocol.block;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class WriteFileResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("***********END***********");
    }

    public WriteFileResp(RespType responseType) {
        super(responseType);
    }

    public WriteFileResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

}
