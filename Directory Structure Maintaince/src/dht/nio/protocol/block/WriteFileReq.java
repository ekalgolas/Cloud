package dht.nio.protocol.block;

import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ReqType;

public class WriteFileReq extends ProtocolReq{
	private static final long serialVersionUID = 1L;

    private String blkName;
    private String token;
    private long baseBlkVersion;// the base version to modify
    private byte[] buf;
    private long pos;
    private boolean isInsert;
    private int len;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("blkName: " + blkName);
        System.out.println("token: " + token);
        System.out.println("baseBlkVersion: " + baseBlkVersion);
        System.out.println("buf: " + buf);
        System.out.println("pos: " + pos);
        System.out.println("isInsert: " + isInsert);
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
    }

    public long getBaseBlkVersion() {
        return baseBlkVersion;
    }

    public void setBaseBlkVersion(long baseBlkVersion) {
        this.baseBlkVersion = baseBlkVersion;
    }

    public String getBlkName() {
        return blkName;
    }

    public void setBlkName(String blkName) {
        this.blkName = blkName;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public boolean isInsert() {
        return isInsert;
    }

    public void setInsert(boolean isInsert) {
        this.isInsert = isInsert;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

	public int getLen() {
		return len;
	}

	public void setLen(int len) {
		this.len = len;
	}

}
