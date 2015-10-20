package master.dht.nio.protocol.block;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class WriteInitResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;
    private String transactionId;
    private long blkVersion;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("transactionId: " + transactionId);
        System.out.println("blkVersion: " + blkVersion);
        System.out.println("***********END***********");
    }

    public WriteInitResp(RespType responseType) {
        super(responseType);
    }

    public WriteInitResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public long getBlkVersion() {
        return blkVersion;
    }

    public void setBlkVersion(long blkVersion) {
        this.blkVersion = blkVersion;
    }

}
