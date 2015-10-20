package master.dht.nio.protocol.meta;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class OpenFileReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;

    private String fileName;

    private long bytesToAdd;
    private int preferredBlkSize;

    private boolean writeLock;
    private boolean appendLock;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("fileName: " + fileName);
        System.out.println("bytesToAdd: " + bytesToAdd);
        System.out.println("preferredBlkSize: " + preferredBlkSize);
        System.out.println("writeLock: " + writeLock);
        System.out.println("appendLock: " + appendLock);
        System.out.println("***********END***********");
    }

    public OpenFileReq(ReqType requestType) {
        super(requestType);
    }

    public OpenFileReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getBytesToAdd() {
        return bytesToAdd;
    }

    public void setBytesToAdd(long bytesToAdd) {
        this.bytesToAdd = bytesToAdd;
    }

    public int getPreferredBlkSize() {
        return preferredBlkSize;
    }

    public void setPreferredBlkSize(int preferredBlkSize) {
        this.preferredBlkSize = preferredBlkSize;
    }

    public boolean isWriteLock() {
        return writeLock;
    }

    public void setWriteLock(boolean writeLock) {
        this.writeLock = writeLock;
    }

    public boolean isAppendLock() {
        return appendLock;
    }

    public void setAppendLock(boolean appendLock) {
        this.appendLock = appendLock;
    }

}
