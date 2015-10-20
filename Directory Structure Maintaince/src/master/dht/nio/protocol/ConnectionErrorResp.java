package master.dht.nio.protocol;

public class ConnectionErrorResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    private String uid;

    public ConnectionErrorResp(RespType responseType) {
        super(responseType);
    }

    public ConnectionErrorResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

}
