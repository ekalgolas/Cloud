package dht.swift.server.proxy;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.table.PhysicalNode;
import dht.dhtfs.core.table.RouteTable;
import dht.nio.client.TCPClient;
import dht.nio.client.TCPConnection;
import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.RespType;
import dht.nio.protocol.meta.CreateFileReq;
import dht.nio.protocol.meta.CreateFileResp;
import dht.nio.protocol.meta.OpenFileReq;
import dht.nio.protocol.meta.OpenFileResp;
import dht.nio.protocol.table.JoinReq;
import dht.nio.protocol.table.JoinResp;
import dht.nio.protocol.table.TableReq;
import dht.nio.protocol.table.TableResp;
import dht.nio.server.ConnectionInfo;
import dht.nio.server.IProcessor;

public class ProxyRequestProcessor implements IProcessor {

	protected Configuration conf;
	protected TCPClient client;
	protected RouteTable table;

	@Override
	public void initialize(Configuration config) throws IOException {
		conf = config;
		client = new TCPClient();
		try {
			table = (RouteTable) RouteTable.loadMeta(config.getProperty("imgFile"));
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
		case CREATE_FILE:
			resp = handleCreateFileReq(info, (CreateFileReq) req);
			break;
		case OPEN_FILE:
			resp = handleOpenFileReq(info, (OpenFileReq) req);
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

	public ProtocolResp handleCreateFileReq(ConnectionInfo info, CreateFileReq createReq) {
		CreateFileResp createResp = new CreateFileResp(RespType.OK);

		DhtPath path = new DhtPath(createReq.getFileName());
		PhysicalNode node = table.getPrimaryNode(path);

		TCPConnection connection;
		try {
			connection = TCPConnection.getInstance(node.getIpAddress(), node.getPort());

			CreateFileReq req = new CreateFileReq(ReqType.CREATE_FILE);
			req.setFileName(path.getAbsolutePath());
			req.setNewBlkNum(1);
			connection.request(req);
			CreateFileResp resp = (CreateFileResp) connection.response();
			if (resp.getResponseType() != RespType.OK) {
				createResp.setResponseType(RespType.IOERROR);
				createResp.setMsg("create file " + path.getPath() + " failed, error: " + createResp.getResponseType()
						+ " msg: " + resp.getMsg());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return createResp;
	}

	public ProtocolResp handleOpenFileReq(ConnectionInfo info, OpenFileReq openReq) {
		OpenFileResp openResp = new OpenFileResp(RespType.OK);

		DhtPath path = new DhtPath(openReq.getFileName());
		PhysicalNode node = table.getPrimaryNode(path);

		TCPConnection connection;
		try {
			connection = TCPConnection.getInstance(node.getIpAddress(), node.getPort());
			OpenFileReq req = new OpenFileReq(ReqType.OPEN_FILE);
			req.setFileName(path.getPath());
			connection.request(req);
			OpenFileResp resp = (OpenFileResp) connection.response();
			if (resp.getResponseType() != RespType.OK) {
				openResp.setResponseType(RespType.IOERROR);
				openResp.setMsg("open file " + path.getPath() + " failed, error: " + resp.getResponseType() + " msg: "
						+ resp.getMsg());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return openResp;
	}
}
