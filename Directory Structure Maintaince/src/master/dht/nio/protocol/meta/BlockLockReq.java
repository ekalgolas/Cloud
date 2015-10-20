package master.dht.nio.protocol.meta;

import java.util.List;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class BlockLockReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;

    private String fileName;
    private List<String> blkNames;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("***********END***********");
    }

    public BlockLockReq(ReqType requestType) {
        super(requestType);
    }

    public BlockLockReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public List<String> getBlkNames() {
        return blkNames;
    }

    public void setBlkNames(List<String> blkNames) {
        this.blkNames = blkNames;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

}
