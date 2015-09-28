package dht.nio.protocol.table;

import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ReqType;

public class TableReq extends ProtocolReq {

	private static final long serialVersionUID = 1L;

	public TableReq(ReqType requestType) {
		super(requestType);
	}

	public TableReq(int rId, ReqType requestType) {
		super(rId, requestType);
	}

}
