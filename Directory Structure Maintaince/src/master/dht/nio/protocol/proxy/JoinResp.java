package master.dht.nio.protocol.proxy;

import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.dhtfs.core.table.RouteTable;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class JoinResp extends ProtocolResp {

	private static final long serialVersionUID = 1L;

	private RouteTable table;

	private PhysicalNode local;

	public JoinResp(RespType responseType) {
		super(responseType);
	}

	public JoinResp(int rId, RespType responseType) {
		super(rId, responseType);
	}

	public RouteTable getTable() {
		return table;
	}

	public void setTable(RouteTable table) {
		this.table = table;
	}

	public PhysicalNode getLocal() {
		return local;
	}

	public void setLocal(PhysicalNode local) {
		this.local = local;
	}

}
