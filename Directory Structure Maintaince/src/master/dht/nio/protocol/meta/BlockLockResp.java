package master.dht.nio.protocol.meta;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class BlockLockResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("***********END***********");
    }

    public BlockLockResp(RespType responseType) {
        super(responseType);
    }

    public BlockLockResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

}
