package master.dht.nio.protocol.dir;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class RemoveFileReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;
    private String dirKey;
    private String fileName;

    public RemoveFileReq(ReqType requestType) {
        super(requestType);
    }

    public RemoveFileReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getDirKey() {
        return dirKey;
    }

    public void setDirKey(String dirKey) {
        this.dirKey = dirKey;
    }

}
