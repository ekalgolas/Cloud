package dht.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.table.PhysicalNode;
import dht.hdfs.core.table.DatanodeManager;
import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.RespType;
import dht.nio.protocol.block.ReadFileReq;
import dht.nio.protocol.block.ReadFileResp;
import dht.nio.protocol.meta.CreateFileReq;
import dht.nio.protocol.meta.CreateFileResp;
import dht.nio.protocol.meta.OpenFileReq;
import dht.nio.protocol.meta.OpenFileResp;
import dht.nio.protocol.table.JoinReq;
import dht.nio.protocol.table.JoinResp;
import dht.nio.server.ConnectionInfo;
import dht.nio.server.IProcessor;

public class NameNodeRequestProcessor implements IProcessor {
	protected Configuration conf;
	protected DatanodeManager datanodeManager;
	protected FSNamesystem namesystem;

	@Override
	public void initialize(Configuration config) throws IOException {
		this.conf = config;
		try {
			datanodeManager = (DatanodeManager) DatanodeManager
					.loadMeta(this.conf.getProperty("imgFile"));
		} catch (IOException e) {
			datanodeManager = new DatanodeManager();
			datanodeManager.initialize(config);
		}

		namesystem = FSNamesystem.loadFromDisk();
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
		case CREATE_FILE:
			resp = handleCreateFile(info, (CreateFileReq) req);
			break;
		case OPEN_FILE:
			resp = handleOpenFile(info, (OpenFileReq) req);
			break;
		default:
			;
		}
		resp.setrId(req.getrId());
		return resp;
	}

	private ProtocolResp handleJoin(ConnectionInfo info, JoinReq req) {
		JoinResp resp = new JoinResp(RespType.OK);
		PhysicalNode joinNode = new PhysicalNode(info.getIp(), req.getPort());
		GeometryLocation location = req.getLocation();
		joinNode.setLocation(location);
		datanodeManager.join(joinNode);
		resp.setLocal(joinNode);
		return resp;
	}

	private ProtocolResp handleCreateFile(ConnectionInfo info, CreateFileReq req) {
		CreateFileResp resp = new CreateFileResp(RespType.OK);
		String path = req.getFileName();
		String userName = "user1";
		String client = "client1";
		boolean createParent = true;
		short replication = 3;
		long blockSize = 64 * 1024 * 1024;

		try {
			namesystem.startFile(path, userName, client, createParent,
					replication, blockSize);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return resp;
	}

	private OpenFileResp handleOpenFile(ConnectionInfo info, OpenFileReq req) {
		OpenFileResp resp = new OpenFileResp(RespType.OK);
		String path = req.getFileName();
		String client = "client1";
		try {
			long endTime = namesystem.getBlockLocationsUpdateTimes(path, 0,
					1000, false, false);
			resp.setFileName(req.getFileName());
			resp.setFileSize(endTime);

			// if (blocks != null) {
			// resp.setBlkNum(blocks.getLocatedBlocks().size());
			// List<String> newBlkNames = new ArrayList<String>();
			// for (LocatedBlock lb : blocks.getLocatedBlocks()) {
			// newBlkNames.add(lb.toString());
			// }
			// resp.setNewBlkNames(newBlkNames);
			// }

			return resp;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resp;
	}
}
