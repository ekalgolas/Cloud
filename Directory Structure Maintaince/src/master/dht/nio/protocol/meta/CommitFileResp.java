package master.dht.nio.protocol.meta;

import master.dht.dhtfs.server.datanode.FileMeta;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class CommitFileResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    private FileMeta fileMeta;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("fileMeta: " + fileMeta.toString());
        System.out.println("***********END***********");
    }

    public CommitFileResp(RespType responseType) {
        super(responseType);
    }

    public CommitFileResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public FileMeta getFileMeta() {
        return fileMeta;
    }

    public void setFileMeta(FileMeta fileMeta) {
        this.fileMeta = fileMeta;
    }

}
