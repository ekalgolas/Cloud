package dht.nio.protocol;

public class ExistFileReq extends ProtocolReq {

	private static final long serialVersionUID = 1L;
	private String fileName;

	public ExistFileReq(ReqType requestType) {
		super(requestType);
	}

	public ExistFileReq(int rId, ReqType requestType) {
		super(rId, requestType);
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

}
