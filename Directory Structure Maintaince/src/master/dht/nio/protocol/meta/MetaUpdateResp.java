package master.dht.nio.protocol.meta;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class MetaUpdateResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("***********END***********");
    }

    public MetaUpdateResp(RespType responseType) {
        super(responseType);
    }

    public MetaUpdateResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

}
