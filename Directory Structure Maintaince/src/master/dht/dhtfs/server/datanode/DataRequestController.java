package master.dht.dhtfs.server.datanode;

import java.io.File;
import java.io.IOException;

import master.dht.dhtfs.client.ClientConfiguration;
import master.dht.dhtfs.core.GeometryLocation;
import master.dht.dhtfs.core.table.CachedRouteTable;
import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.client.TCPClient;
import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.block.ReadFileReq;
import master.dht.nio.protocol.block.WriteFileReq;
import master.dht.nio.protocol.block.WriteFinishReq;
import master.dht.nio.protocol.block.WriteInitReq;
import master.dht.nio.protocol.dir.AddFileReq;
import master.dht.nio.protocol.dir.ListStatusReq;
import master.dht.nio.protocol.dir.RemoveFileReq;
import master.dht.nio.protocol.meta.BlockNameReq;
import master.dht.nio.protocol.meta.CommitFileReq;
import master.dht.nio.protocol.meta.CreateFileReq;
import master.dht.nio.protocol.meta.DeleteFileReq;
import master.dht.nio.protocol.meta.MetaUpdateReq;
import master.dht.nio.protocol.meta.OpenFileReq;
import master.dht.nio.protocol.proxy.HeartBeatReq;
import master.dht.nio.protocol.proxy.HeartBeatResp;
import master.dht.nio.protocol.proxy.JoinReq;
import master.dht.nio.protocol.proxy.JoinResp;
import master.dht.nio.protocol.proxy.TableReq;
import master.dht.nio.protocol.proxy.TableResp;
import master.dht.nio.server.ConnectionInfo;
import master.dht.nio.server.IController;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataRequestController implements IController {

	Log log = LogFactory.getLog("dir");
	protected CachedRouteTable cachedTable;
	protected PhysicalNode local;

	protected MetaRequestProcessor metaProcessor;
	protected BlockRequestProcessor blockProcessor;
	protected DirectoryRequestProcessor dirProcessor;

	@Override
	public void initialize() throws IOException {
		PhysicalNode master = new PhysicalNode(
				DataServerConfiguration.getMasterIp(),
				DataServerConfiguration.getMasterPort());
		join(master);
	}

	void join(PhysicalNode master) throws IOException {
		TCPConnection connection = TCPConnection.getInstance(
				master.getIpAddress(), master.getPort());
		JoinReq req = new JoinReq(ReqType.JOIN);
		GeometryLocation location = new GeometryLocation(
				ClientConfiguration.getLatitude(),
				ClientConfiguration.getLongitude());
		req.setLocation(location);
		req.setPort(DataServerConfiguration.getPort());
		req.setUid(getUid());
		connection.request(req);
		JoinResp resp = (JoinResp) connection.response();
		connection.close();
		local = resp.getLocal();
		if (local.getLocation() == null) {
			local.setLocation(location);
		}
		saveUid(local.getUid());
		cachedTable = new CachedRouteTable(master);
		cachedTable.updateRouteTable();
		cachedTable.getTable().dump();
		metaProcessor = new MetaRequestProcessor(cachedTable, local);
		blockProcessor = new BlockRequestProcessor(cachedTable, local);
		dirProcessor = new DirectoryRequestProcessor(cachedTable, local);
		metaProcessor.initialize();
		blockProcessor.initialize();
		dirProcessor.initialize();
	}

	String getUid() throws IOException {
		String file = DataServerConfiguration.getIdDir() + "/uid";
		if (!new File(file).exists()) {
			return null;
		}
		ServerUid uid = (ServerUid) ServerUid.loadMeta(file);
		return uid.getServerId();
	}

	void saveUid(String id) throws IOException {
		String file = DataServerConfiguration.getIdDir() + "/uid";
		ServerUid uid = new ServerUid(id);
		uid.save(file);
	}

	@Override
	public ProtocolResp process(ConnectionInfo info, ProtocolReq req) {
		ReqType reqType = req.getRequestType();
		System.out.println("rid: " + req.getrId() + " " + reqType);
		// System.out.println(reqType);
		ProtocolResp resp = null;
		switch (reqType) {
		case TABLE:// ask for the mapping table
			resp = handleTableReq(info, (TableReq) req);
			break;
		case CREATE_FILE:// create a meta file
			resp = metaProcessor.handleCreateFileReq(info, (CreateFileReq) req);
			break;
		case OPEN_FILE:// read the meta file
			resp = metaProcessor.handleOpenFileReq(info, (OpenFileReq) req);
			break;
		case DELETE_FILE:// delete the meta file
			resp = metaProcessor.handleDeleteFileReq(info, (DeleteFileReq) req);
			break;
		case COMMIT_FILE:// write the meta file
			resp = metaProcessor.handleCommitFileReq(info, (CommitFileReq) req);
			break;
		case NEW_BLOCK:
			resp = metaProcessor.handleNewBlockReq(info, (BlockNameReq) req);
			break;
		case READ_FILE:// read the block
			resp = blockProcessor.handleReadBlkReq(info, (ReadFileReq) req);
			break;
		case WRITE_INIT:
			resp = blockProcessor.handleWriteInitReq(info, (WriteInitReq) req);
			break;
		case WRITE_FILE:// update the block
			resp = blockProcessor.handleWriteBlkReq(info, (WriteFileReq) req);
			break;
		case WRITE_FINISH:
			resp = blockProcessor.handleWriteFinishReq(info,
					(WriteFinishReq) req);
			break;
		case DIR_ADD_FILE:
			resp = dirProcessor.handleAddFile(info, (AddFileReq) req);
			break;
		case DIR_LIST_STATUS:
			resp = dirProcessor.handleListStatus(info, (ListStatusReq) req);
			break;
		case DIR_DELETE_FILE:
			resp = dirProcessor.handleRemoveFile(info, (RemoveFileReq) req);
			break;
		case META_UPDATE:
			resp = metaProcessor.handleMetaUpdateReq(info, (MetaUpdateReq) req);
			break;
		case HEART_BEAT:
			resp = handleHeartBeatReq(info, (HeartBeatReq) req);
			break;
		default:
			resp = new ProtocolResp(RespType.UNRECOGNIZE);
			resp.setMsg("unrecognized request type, type: "
					+ req.getRequestType());
		}
		if (resp != null) {
			resp.setrId(req.getrId());
		}
		// System.out.println("resp generated");
		return resp;
	}

	ProtocolResp handleTableReq(ConnectionInfo info, TableReq req) {
		TableResp resp = new TableResp(RespType.OK);
		resp.setTable(cachedTable.getTable());
		return resp;
	}

	ProtocolResp handleHeartBeatReq(ConnectionInfo info, HeartBeatReq req) {
		HeartBeatResp resp = new HeartBeatResp(RespType.OK);
		resp.setUid(local.getUid());
		return resp;
	}

	@Override
	public ProtocolReq process(TCPClient client, ProtocolResp resp) {
		// TODO Auto-generated method stub
		return null;
	}

	// private DhtPath createTmpFile(String fileName) throws IOException {
	// File f = new File(conf.getProperty("tmpDir"));
	// return new DhtPath(File.createTempFile(fileName, ".tmp", f)
	// .getCanonicalPath());
	// }
}
