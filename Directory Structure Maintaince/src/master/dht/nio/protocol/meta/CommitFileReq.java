package master.dht.nio.protocol.meta;

import java.util.List;

import master.dht.dhtfs.server.datanode.FileMeta;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class CommitFileReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;

    private FileMeta fileMeta;
    private List<String> locks;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("fileMeta: " + fileMeta.toString());
        System.out.println("***********END***********");
    }

    public CommitFileReq(ReqType requestType) {
        super(requestType);
    }

    public CommitFileReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public FileMeta getFileMeta() {
        return fileMeta;
    }

    public void setFileMeta(FileMeta fileMeta) {
        this.fileMeta = fileMeta;
    }

    public List<String> getLocks() {
        return locks;
    }

    public void setLocks(List<String> locks) {
        this.locks = locks;
    }

}
