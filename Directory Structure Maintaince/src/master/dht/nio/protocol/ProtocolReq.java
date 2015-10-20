package master.dht.nio.protocol;

import java.io.Serializable;

public class ProtocolReq implements Serializable {

	private static final long serialVersionUID = 1L;

	private int rId;

	private ReqType requestType;

	private boolean asynchronous;

	public void dump() {
//		System.out.println("rId: " + rId);
		System.out.println("requestType: " + requestType);
	}

	public ProtocolReq(ReqType requestType) {
		this.requestType = requestType;
		this.asynchronous = false;
	}

	public ProtocolReq(int rId, ReqType requestType) {
		this.setrId(rId);
		this.requestType = requestType;
		this.asynchronous = false;
	}

	public ReqType getRequestType() {
		return requestType;
	}

	public void setRequestType(ReqType requestType) {
		this.requestType = requestType;
	}

	public String toString() {
		return "rId: " + rId + " requestType: " + requestType;
	}

	public int getrId() {
		return rId;
	}

	public void setrId(int rId) {
		this.rId = rId;
	}

	public boolean isAsynchronous() {
		return asynchronous;
	}

	public void setAsynchronous(boolean asynchronous) {
		this.asynchronous = asynchronous;
	}

}
