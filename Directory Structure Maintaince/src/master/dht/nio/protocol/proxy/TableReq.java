package master.dht.nio.protocol.proxy;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class TableReq extends ProtocolReq {

	private static final long serialVersionUID = 1L;

	public TableReq(ReqType requestType) {
		super(requestType);
	}

	public TableReq(int rId, ReqType requestType) {
		super(rId, requestType);
	}

}
