package dht.nio.protocol.block;

import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.RespType;

public class ReadFileResp extends ProtocolResp {

	private static final long serialVersionUID = 1L;

	private byte[] buf;

	private boolean eof;

	public void dump() {
		System.out.println("***********BEGIN***********");
		super.dump();
		System.out.println("buf: " + new String(buf));
		System.out.println("eof: " + eof);
		System.out.println("***********END***********");
	}

	public ReadFileResp(RespType responseType) {
		super(responseType);
	}

	public ReadFileResp(int rId, RespType responseType) {
		super(rId, responseType);
	}

	public byte[] getBuf() {
		return buf;
	}

	public void setBuf(byte[] buf) {
		this.buf = buf;
	}

	public boolean isEof() {
		return eof;
	}

	public void setEof(boolean eof) {
		this.eof = eof;
	}

	@Override
	public String toString() {
		return "ReadFileResp [buf=" + buf.length + ", eof=" + eof + "]";
	}

}
