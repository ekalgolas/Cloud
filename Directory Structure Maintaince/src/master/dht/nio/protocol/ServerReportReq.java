package master.dht.nio.protocol;

public class ServerReportReq extends ProtocolReq {

	private static final long serialVersionUID = 1L;

	public ServerReportReq(ReqType requestType) {
		super(requestType);
	}

	public ServerReportReq(int rId, ReqType requestType) {
		super(rId, requestType);
	}

	public String toString() {
		String s = super.toString();
		return s;
	}

}
