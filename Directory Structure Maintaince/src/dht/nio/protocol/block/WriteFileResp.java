package dht.nio.protocol.block;

import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.RespType;

public class WriteFileResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;
    private long fileSize;
    private String token;
    private String checkSum;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("fileSize: " + fileSize);
        System.out.println("token: " + token);
        System.out.println("checkSum: " + checkSum);
        System.out.println("***********END***********");
    }

    public WriteFileResp(RespType responseType) {
        super(responseType);
    }

    public WriteFileResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getCheckSum() {
        return checkSum;
    }

    public void setCheckSum(String checkSum) {
        this.checkSum = checkSum;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

}
