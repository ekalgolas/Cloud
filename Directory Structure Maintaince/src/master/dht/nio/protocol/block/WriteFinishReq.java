package master.dht.nio.protocol.block;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class WriteFinishReq extends ProtocolReq {
    private static final long serialVersionUID = 1L;

    private String transactionId;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("transactionId: " + transactionId);
        System.out.println("***********END***********");
    }

    public WriteFinishReq(ReqType requestType) {
        super(requestType);
    }

    public WriteFinishReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

}