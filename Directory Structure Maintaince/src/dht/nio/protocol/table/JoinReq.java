package dht.nio.protocol.table;

import dht.dhtfs.core.GeometryLocation;
import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ReqType;

public class JoinReq extends ProtocolReq {

	private static final long serialVersionUID = 1L;
	private GeometryLocation location;
	private int port;

	public JoinReq(ReqType requestType) {
		super(requestType);
	}

	public JoinReq(int rId, ReqType requestType) {
		super(rId, requestType);
	}

	public GeometryLocation getLocation() {
		return location;
	}

	public void setLocation(GeometryLocation location) {
		this.location = location;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

}
