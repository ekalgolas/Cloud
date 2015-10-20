package master.dht.nio.protocol.dir;

import master.dht.dhtfs.server.datanode.FileInfo;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class ListStatusResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;
    private FileInfo fileInfo;

    public ListStatusResp(RespType responseType) {
        super(responseType);
    }

    public ListStatusResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(FileInfo fileInfo) {
        this.fileInfo = fileInfo;
    }
}
