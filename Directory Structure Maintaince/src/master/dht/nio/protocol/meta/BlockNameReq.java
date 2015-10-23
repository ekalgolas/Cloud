package master.dht.nio.protocol.meta;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class BlockNameReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;

    private long bytesToAdd;
    private int preferredBlkSize;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("bytesToAdd: " + bytesToAdd);
        System.out.println("preferredBlkSize: " + preferredBlkSize);
        System.out.println("***********END***********");
    }

    public BlockNameReq(ReqType requestType) {
        super(requestType);
    }

    public BlockNameReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public int getPreferredBlkSize() {
        return preferredBlkSize;
    }

    public void setPreferredBlkSize(int preferredBlkSize) {
        this.preferredBlkSize = preferredBlkSize;
    }

    public long getBytesToAdd() {
        return bytesToAdd;
    }

    public void setBytesToAdd(long bytesToAdd) {
        this.bytesToAdd = bytesToAdd;
    }

}
