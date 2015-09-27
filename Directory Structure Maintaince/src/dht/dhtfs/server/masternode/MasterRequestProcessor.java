package dht.dhtfs.server.masternode;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.table.PhysicalNode;
import dht.dhtfs.core.table.RouteTable;
import dht.nio.client.TCPClient;
import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.RespType;
import dht.nio.protocol.table.JoinReq;
import dht.nio.protocol.table.JoinResp;
import dht.nio.protocol.table.TableReq;
import dht.nio.protocol.table.TableResp;
import dht.nio.server.ConnectionInfo;
import dht.nio.server.IProcessor;

public class MasterRequestProcessor implements IProcessor {

	protected Configuration conf;
	protected TCPClient client;
	protected RouteTable table;

    @Override
	public void initialize(Configuration config) throws IOException {
		conf = config;
		client = new TCPClient();
		try {
			table = (RouteTable) RouteTable.loadMeta(config
					.getProperty("imgFile"));
		} catch (IOException e) {
			table = new RouteTable();
			table.initialize(config);
		}
	}

	@Override
	public ProtocolResp process(ConnectionInfo info, ProtocolReq req) {
		ReqType reqType = req.getRequestType();
		System.out.println(reqType);
		ProtocolResp resp = null;
		switch (reqType) {
		case JOIN:
			resp = handleJoin(info, (JoinReq) req);
			break;
		case TABLE:
			resp = handleTableReq(info, (TableReq) req);
			break;
		default:
			;
		}
		resp.setrId(req.getrId());
		return resp;
	}

	public ProtocolResp handleJoin(ConnectionInfo info, JoinReq req) {
		JoinResp resp = new JoinResp(RespType.OK);
		PhysicalNode joinNode = new PhysicalNode(info.getIp(), req.getPort());
		GeometryLocation location = req.getLocation();
		joinNode.setLocation(location);
		table.join(joinNode);
		resp.setTable(table);
		resp.setLocal(joinNode);
		return resp;
	}

	public ProtocolResp handleTableReq(ConnectionInfo info, TableReq req) {
		TableResp resp = new TableResp(RespType.OK);
		resp.setTable(table);
		return resp;
	}
}
