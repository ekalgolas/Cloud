package dht.nio.protocol.block;

import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ReqType;

public class ReadFileReq extends ProtocolReq {
    private static final long serialVersionUID = 1L;

    private String blkName;
    private long blkVersion;
    private long pos;
    private int len;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("blkName: " + blkName);
        System.out.println("blkVersion: " + blkVersion);
        System.out.println("pos: " + pos);
        System.out.println("len: " + len);
        System.out.println("***********END***********");
    }

    public ReadFileReq(ReqType requestType) {
        super(requestType);
    }

    public ReadFileReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public String getBlkName() {
        return blkName;
    }

    public void setBlkName(String blkName) {
        this.blkName = blkName;
    }

    public long getBlkVersion() {
        return blkVersion;
    }

    public void setBlkVersion(long blkVersion) {
        this.blkVersion = blkVersion;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

}
