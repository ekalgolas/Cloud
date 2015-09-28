package dht.nio.protocol.meta;

import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ReqType;

public class CreateFileReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;

    private String fileName;
    private int newBlkNum;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("fileName: " + fileName);
        System.out.println("newBlkNum: " + newBlkNum);
        System.out.println("***********END***********");
    }

    public CreateFileReq(ReqType requestType) {
        super(requestType);
    }

    public CreateFileReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getNewBlkNum() {
        return newBlkNum;
    }

    public void setNewBlkNum(int newBlkNum) {
        this.newBlkNum = newBlkNum;
    }

}
