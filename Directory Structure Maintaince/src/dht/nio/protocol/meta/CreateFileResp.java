package dht.nio.protocol.meta;

import java.util.List;

import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.RespType;

public class CreateFileResp extends ProtocolResp {

	private static final long serialVersionUID = 1L;

	private List<String> newBlkNames;// names of the new blocks to write data

	public void dump() {
		System.out.println("***********BEGIN***********");
		super.dump();
		dumpStr("newBlkNames", newBlkNames);
		System.out.println("***********END***********");
	}

	public CreateFileResp(RespType responseType) {
		super(responseType);
	}

	public CreateFileResp(int rId, RespType responseType) {
		super(rId, responseType);
	}

	public List<String> getNewBlkNames() {
		return newBlkNames;
	}

	public void setNewBlkNames(List<String> newBlkNames) {
		this.newBlkNames = newBlkNames;
	}

}
