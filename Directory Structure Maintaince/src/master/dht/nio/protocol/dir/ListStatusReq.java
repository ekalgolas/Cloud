package master.dht.nio.protocol.dir;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class ListStatusReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;
    private String fileName;

    public ListStatusReq(ReqType requestType) {
        super(requestType);
    }

    public ListStatusReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

}
