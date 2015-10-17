package dht.nio.protocol.meta;

import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.RespType;

public class DeleteFileResp extends ProtocolResp {

	private static final long serialVersionUID = 1L;

	public void dump() {
		System.out.println("***********BEGIN***********");
		super.dump();
		System.out.println("***********END***********");
	}

	public DeleteFileResp(RespType responseType) {
		super(responseType);
	}

	public DeleteFileResp(int rId, RespType responseType) {
		super(rId, responseType);
	}
}
