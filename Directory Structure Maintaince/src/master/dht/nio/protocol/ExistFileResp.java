package master.dht.nio.protocol;

public class ExistFileResp extends ProtocolResp {

	private static final long serialVersionUID = 1L;

	public ExistFileResp(RespType responseType) {
		super(responseType);
	}

	public ExistFileResp(int rId, RespType responseType) {
		super(rId, responseType);
	}

}
