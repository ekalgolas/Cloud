package dht.hdfs.server.datanode;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.table.PhysicalNode;
import dht.nio.client.TCPConnection;
import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.RespType;
import dht.nio.protocol.table.JoinReq;
import dht.nio.protocol.table.JoinResp;
import dht.nio.server.ConnectionInfo;
import dht.nio.server.IProcessor;

public class DatanodeRequestProcessor implements IProcessor {

	protected Configuration conf;
	protected PhysicalNode local;
	protected PhysicalNode master;

	@Override
	public void initialize(Configuration config) throws IOException {
		this.conf = config;
		String masterIp = conf.getProperty("masterIp");
		int masterPort = Integer.parseInt(conf.getProperty("masterPort"));
		master = new PhysicalNode(masterIp, masterPort);
		join();
	}

	void join() throws IOException {
		TCPConnection connection = openConnection(master);
		JoinReq req = new JoinReq(ReqType.JOIN);
		double x = Double.parseDouble(conf.getProperty("locationX"));
		double y = Double.parseDouble(conf.getProperty("locationY"));
		GeometryLocation location = new GeometryLocation(x, y);
		req.setLocation(location);
		req.setPort(Integer.parseInt(conf.getProperty("port")));
		connection.request(req);
		JoinResp resp = (JoinResp) connection.response();

		if (resp.getResponseType() != RespType.OK) {
			throw new IOException("Not able to register DataNode to master");
		}

		local = resp.getLocal();
		if (local.getLocation() == null) {
			local.setLocation(location);
		}
		connection.close();
	}

	TCPConnection openConnection(PhysicalNode node) throws IOException {
		TCPConnection connection = TCPConnection.getInstance(node.getIpAddress(), node.getPort());
		return connection;
	}

	@Override
	public ProtocolResp process(ConnectionInfo info, ProtocolReq req) {
		ReqType reqType = req.getRequestType();
		System.out.println(reqType);
		ProtocolResp resp = null;
		switch (reqType) {
		case OPEN_FILE:// read the meta file
			// resp = handleOpenFileReq(info, (OpenFileReq) req);
			break;
		case READ_FILE:// read the block
			// resp = handleReadBlkReq(info, (ReadFileReq) req);
			break;
		case WRITE_FILE:// update the block
			// resp = handleWriteBlkReq(info, (WriteFileReq) req);
			break;
		case DELETE_FILE:// delete the meta file
			// resp = handleDeleteFileReq(info, (DeleteFileReq) req);
			break;
		case COMMIT_FILE:// write the meta file
			// resp = handleCommitFileReq(info, (CommitFileReq) req);
			break;
		default:
			resp = new ProtocolResp(RespType.UNRECOGNIZE);
			resp.setMsg("unrecognized request type, type: " + req.getRequestType());
		}
		resp.setrId(req.getrId());
		return resp;
	}

}
