package master.dht.nio.protocol.block;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class WriteFileReq extends ProtocolReq {
    private static final long serialVersionUID = 1L;

    private String transactionId;
    private byte[] buf;
    private int len;
    private long pos;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("transactionId: " + transactionId);
        // System.out.println("buf: " + buf);
        System.out.println("pos: " + pos);
        System.out.println("***********END***********");
    }

    public WriteFileReq(ReqType requestType) {
        super(requestType);
    }

    public WriteFileReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public byte[] getBuf() {
        return buf;
    }

    public void setBuf(byte[] buf) {
        this.buf = buf;
        if (buf != null) {
            this.len = buf.length;
        }
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

}
