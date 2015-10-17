package dht.swift.server.proxy.direct;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.FileLockManager;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.LocalMetaFileSystem;
import dht.dhtfs.core.def.IFile;
import dht.dhtfs.core.def.IFileSystem;
import dht.dhtfs.core.def.ILockManager;
import dht.dhtfs.core.table.PhysicalNode;
import dht.dhtfs.core.table.RouteTable;
import dht.nio.client.TCPClient;
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
import dht.swift.server.datanode.FileMeta;

public class ProxyRequestProcessor implements IProcessor {

	protected Configuration conf;
	protected TCPClient client;
	protected RouteTable table;
	protected IFileSystem metaFileSystem;
	protected ILockManager metaLockManager;

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
		metaFileSystem = new LocalMetaFileSystem();
		metaLockManager = new FileLockManager();
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

		// DhtPath mappingPath = new DhtPath(DataRequestProcessor.dataDir + "/"
		// + path.getName());
		DhtPath mappingPath = path.getMappingPath();
		FileMeta fileMeta = new FileMeta(createReq.getFileName());

		try {
			metaLockManager.acquireWriteLock(mappingPath.getAbsolutePath());
			IFile metaFile = metaFileSystem.create(mappingPath);
			metaFile.write(fileMeta.toByteArray());
			metaFile.close();
		} catch (IOException e) {
			createResp.setResponseType(RespType.IOERROR);
			createResp.setMsg("create meta file failed: " + e.getMessage());
			return createResp;
		} finally {
			metaLockManager.releaseWriteLock(mappingPath.getAbsolutePath());
		}

		path.getName();

		return createResp;
	}

	public ProtocolResp handleOpenFileReq(ConnectionInfo info, OpenFileReq openReq) {
		OpenFileResp openResp = new OpenFileResp(RespType.OK);

		DhtPath path = new DhtPath(openReq.getFileName());
		PhysicalNode node = table.getPrimaryNode(path);

		DhtPath mappingPath = path.getMappingPath();
		FileMeta fileMeta = null;

		try {
			metaLockManager.acquireReadLock(mappingPath.getAbsolutePath());
			IFile metaFile = metaFileSystem.open(mappingPath, IFile.READ);
			byte[] buf = new byte[(int) metaFile.length()];
			metaFile.read(buf);
			fileMeta = (FileMeta) FileMeta.fromBytes(buf);
			metaFile.close();
		} catch (IOException e) {
			e.printStackTrace();
			openResp.setResponseType(RespType.IOERROR);
			openResp.setMsg("open meta file failed");
			return openResp;
		} finally {
			metaLockManager.releaseReadLock(mappingPath.getAbsolutePath());
		}
		if (fileMeta == null) {
			openResp.setResponseType(RespType.IOERROR);
			openResp.setMsg("metaFile format error");
			return openResp;
		}

		return openResp;
	}
}
