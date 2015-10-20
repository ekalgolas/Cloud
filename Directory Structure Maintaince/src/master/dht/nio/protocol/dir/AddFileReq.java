package master.dht.nio.protocol.dir;

import master.dht.dhtfs.server.datanode.FileInfo;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class AddFileReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;
    private String dirKey;
    private FileInfo fileInfo;

    public AddFileReq(ReqType requestType) {
        super(requestType);
    }

    public AddFileReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(FileInfo fileInfo) {
        this.fileInfo = fileInfo;
    }

    public String getDirKey() {
        return dirKey;
    }

    public void setDirKey(String dirKey) {
        this.dirKey = dirKey;
    }

}
