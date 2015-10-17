package dht.nio.protocol.meta;

import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ReqType;

public class DeleteFileReq extends ProtocolReq {

	private static final long serialVersionUID = 1L;

	private String fileName;

	public void dump() {
		System.out.println("***********BEGIN***********");
		super.dump();
		System.out.println("fileName: " + fileName);
		System.out.println("***********END***********");
	}

	public DeleteFileReq(ReqType requestType) {
		super(requestType);
	}

	public DeleteFileReq(int rId, ReqType requestType) {
		super(rId, requestType);
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

}
