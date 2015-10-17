package dht.swift.server.datanode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dht.dhtfs.core.BlockNameAssigner;
import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.FileLockManager;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.LocalDataFileSystem;
import dht.dhtfs.core.LocalMetaFileSystem;
import dht.dhtfs.core.TokenAssigner;
import dht.dhtfs.core.def.IFile;
import dht.dhtfs.core.def.IFileSystem;
import dht.dhtfs.core.def.IIDAssigner;
import dht.dhtfs.core.def.ILockManager;
import dht.dhtfs.core.table.PhysicalNode;
import dht.dhtfs.core.table.RouteTable;
import dht.nio.client.TCPConnection;
import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.RespType;
import dht.nio.protocol.block.ReadFileReq;
import dht.nio.protocol.block.ReadFileResp;
import dht.nio.protocol.block.WriteFileReq;
import dht.nio.protocol.block.WriteFileResp;
import dht.nio.protocol.meta.CommitFileReq;
import dht.nio.protocol.meta.CommitFileResp;
import dht.nio.protocol.meta.CreateFileReq;
import dht.nio.protocol.meta.CreateFileResp;
import dht.nio.protocol.meta.DeleteFileReq;
import dht.nio.protocol.meta.DeleteFileResp;
import dht.nio.protocol.meta.OpenFileReq;
import dht.nio.protocol.meta.OpenFileResp;
import dht.nio.protocol.table.JoinReq;
import dht.nio.protocol.table.JoinResp;
import dht.nio.protocol.table.TableReq;
import dht.nio.protocol.table.TableResp;
import dht.nio.server.ConnectionInfo;
import dht.nio.server.IProcessor;

public class DataRequestProcessor implements IProcessor {

	protected Configuration conf;
	// protected TCPClient client;
	protected RouteTable table;
	protected PhysicalNode local;
	protected PhysicalNode master;

	protected IFileSystem dataFileSystem;
	protected ILockManager dataLockManager;
	protected IFileSystem metaFileSystem;
	protected ILockManager metaLockManager;
	protected IIDAssigner blockNameAssigner;
	protected IIDAssigner tokenAssigner;
	public static String dataDir;

	public DataRequestProcessor() {
		dataFileSystem = new LocalDataFileSystem();
		dataLockManager = new FileLockManager();
		metaFileSystem = new LocalMetaFileSystem();
		metaLockManager = new FileLockManager();
		blockNameAssigner = new BlockNameAssigner();
		tokenAssigner = new TokenAssigner();
	}

	@Override
	public void initialize(Configuration config) throws IOException {
		conf = config;
		dataFileSystem.initialize(config);
		metaFileSystem.initialize(config);
		String masterIp = conf.getProperty("masterIp");
		int masterPort = Integer.parseInt(conf.getProperty("masterPort"));
		master = new PhysicalNode(masterIp, masterPort);
		dataDir = config.getProperty("dataDir");
		// client = new TCPClient();
		join();
	}

	TCPConnection openConnection(PhysicalNode node) throws IOException {
		TCPConnection connection = TCPConnection.getInstance(node.getIpAddress(), node.getPort());
		return connection;
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
		table = resp.getTable();
		local = resp.getLocal();
		if (local.getLocation() == null) {
			local.setLocation(location);
		}
		table.dump();
		connection.close();
	}

	void updateRouteTable() throws IOException {
		TCPConnection connection = openConnection(master);
		TableReq req = new TableReq(ReqType.TABLE);
		connection.request(req);
		TableResp resp = (TableResp) connection.response();
		table = resp.getTable();
		table.dump();
		connection.close();
	}

	@Override
	public ProtocolResp process(ConnectionInfo info, ProtocolReq req) {
		ReqType reqType = req.getRequestType();
		System.out.println(reqType);
		ProtocolResp resp = null;
		switch (reqType) {
		case TABLE:// ask for the mapping table
			resp = handleTableReq(info, (TableReq) req);
			break;
		case CREATE_FILE:// create a meta file
			resp = handleCreateFileReq(info, (CreateFileReq) req);
			break;
		case OPEN_FILE:// read the meta file
			resp = handleOpenFileReq(info, (OpenFileReq) req);
			break;
		case READ_FILE:// read the block
			resp = handleReadBlkReq(info, (ReadFileReq) req);
			break;
		case WRITE_FILE:// update the block
			resp = handleWriteBlkReq(info, (WriteFileReq) req);
			break;
		case DELETE_FILE:// delete the meta file
			resp = handleDeleteFileReq(info, (DeleteFileReq) req);
			break;
		case COMMIT_FILE:// write the meta file
			resp = handleCommitFileReq(info, (CommitFileReq) req);
			break;
		default:
			resp = new ProtocolResp(RespType.UNRECOGNIZE);
			resp.setMsg("unrecognized request type, type: " + req.getRequestType());
		}
		resp.setrId(req.getrId());
		return resp;
	}

	ProtocolResp handleTableReq(ConnectionInfo info, TableReq req) {
		TableResp resp = new TableResp(RespType.OK);
		resp.setTable(table);
		return resp;
	}

	ProtocolResp handleCreateFileReq(ConnectionInfo info, CreateFileReq req) {
		// TODO parameters validation
		CreateFileResp resp = new CreateFileResp(RespType.OK);

		DhtPath sourcePath = new DhtPath(req.getFileName());
		if (!isAvailable(local, sourcePath)) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		DhtPath mappingPath = sourcePath.getMappingPath();
		FileMeta fileMeta = new FileMeta(req.getFileName());
		try {
			metaLockManager.acquireWriteLock(mappingPath.getAbsolutePath());
			IFile metaFile = metaFileSystem.create(mappingPath);
			metaFile.write(fileMeta.toByteArray());
			metaFile.close();
		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("create meta file failed: " + e.getMessage());
			return resp;
		} finally {
			metaLockManager.releaseWriteLock(mappingPath.getAbsolutePath());
		}
		List<String> newBlkNames = new ArrayList<String>();
		for (int i = 0; i < req.getNewBlkNum(); ++i) {
			newBlkNames.add(blockNameAssigner.generateUID());
		}
		resp.setNewBlkNames(newBlkNames);
		return resp;
	}

	ProtocolResp handleOpenFileReq(ConnectionInfo info, OpenFileReq req) {
		// TODO parameters validation
		OpenFileResp resp = new OpenFileResp(RespType.OK);
		DhtPath sourcePath = new DhtPath(req.getFileName());
		if (!isAvailable(local, sourcePath)) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		DhtPath mappingPath = sourcePath.getMappingPath();
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
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("open meta file failed");
			return resp;
		} finally {
			metaLockManager.releaseReadLock(mappingPath.getAbsolutePath());
		}
		if (fileMeta == null) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("metaFile format error");
			return resp;
		}
		long endTime = System.currentTimeMillis();
		resp.setFileName(fileMeta.getFileName());
		// change afterwards
		// resp.setFileSize(fileMeta.getFileSize());
		resp.setFileSize(endTime);
		resp.setBlkNum(fileMeta.getBlkNum());
		resp.setBlkVersions(fileMeta.getBlkVersions());
		resp.setBlkSizes(fileMeta.getBlkSizes());
		resp.setBlkNames(fileMeta.getBlkNames());
		resp.setBlkCheckSums(fileMeta.getBlkCheckSums());
		List<String> newBlkNames = new ArrayList<String>();
		for (int i = 0; i < req.getNewBlkNum(); ++i) {
			newBlkNames.add(blockNameAssigner.generateUID());
		}
		resp.setNewBlkNames(newBlkNames);
		return resp;
	}

	ProtocolResp handleDeleteFileReq(ConnectionInfo info, DeleteFileReq req) {
		// TODO parameters validation
		DeleteFileResp resp = new DeleteFileResp(RespType.OK);
		DhtPath sourcePath = new DhtPath(req.getFileName());
		if (!isAvailable(local, sourcePath)) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		DhtPath mappingPath = sourcePath.getMappingPath();
		try {
			metaLockManager.acquireWriteLock(mappingPath.getAbsolutePath());
			metaFileSystem.delete(mappingPath);
		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("delete meta file failed: " + e.getMessage());
			return resp;
		} finally {
			metaLockManager.releaseWriteLock(mappingPath.getAbsolutePath());
		}
		return resp;
	}

	ProtocolResp handleCommitFileReq(ConnectionInfo info, CommitFileReq req) {
		// TODO parameters validation
		CommitFileResp resp = new CommitFileResp(RespType.OK);
		DhtPath sourcePath = new DhtPath(req.getFileName());
		if (!isAvailable(local, sourcePath)) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		DhtPath mappingPath = sourcePath.getMappingPath();
		FileMeta fileMeta = new FileMeta(req.getFileName());
		fileMeta.setFileSize(req.getFileSize());
		fileMeta.setBlkNum(req.getBlkNum());
		// System.out.println("Version: " + req.getBlkVersions().size() + " "
		// + req.getBlkVersions().get(0));
		fileMeta.setBlkVersions(req.getBlkVersions());
		fileMeta.setBlkSizes(req.getBlkSizes());
		fileMeta.setBlkNames(req.getBlkNames());
		fileMeta.setBlkCheckSums(req.getBlkCheckSums());
		try {
			metaLockManager.acquireWriteLock(mappingPath.getAbsolutePath());
			IFile metaFile = metaFileSystem.open(mappingPath, IFile.WRITE);
			byte[] buf = new byte[(int) metaFile.length()];
			metaFile.read(buf);
			FileMeta baseFileMeta = (FileMeta) FileMeta.fromBytes(buf);
			FileMeta mergedFileMeta = mergeVersion(baseFileMeta, fileMeta);
			metaFile.setLength(0);
			metaFile.write(mergedFileMeta.toByteArray());
			// unlock
			metaFile.close();

			resp.setFileName(mergedFileMeta.getFileName());
			resp.setFileSize(mergedFileMeta.getFileSize());
			resp.setBlkNum(mergedFileMeta.getBlkNum());
			resp.setBlkVersions(mergedFileMeta.getBlkVersions());
			resp.setBlkSizes(mergedFileMeta.getBlkSizes());
			resp.setBlkNames(mergedFileMeta.getBlkNames());
			resp.setBlkCheckSums(mergedFileMeta.getBlkCheckSums());

		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("commit meta file failed: " + e.getMessage());
			return resp;
		} finally {
			metaLockManager.releaseWriteLock(mappingPath.getAbsolutePath());
		}
		return resp;
	}

	ProtocolResp handleReadBlkReq(ConnectionInfo info, ReadFileReq req) {
		// TODO parameters validation
		ReadFileResp resp = new ReadFileResp(RespType.OK);
		DhtPath sourcePath = new DhtPath(req.getBlkName());
		if (!isAvailable(local, sourcePath)) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		DhtPath mappingPath = sourcePath.getMappingPath("_" + req.getBlkVersion());
		try {
			IFile blockFile = dataFileSystem.open(mappingPath);
			int num = (int) Math.min(req.getLen(), blockFile.length() - req.getPos());
			byte[] buf = new byte[Math.max(0, num)];
			blockFile.seek(req.getPos());
			blockFile.read(buf);
			resp.setBuf(buf);
			if (num == blockFile.length() - req.getPos()) {
				resp.setEof(true);
			} else {
				resp.setEof(false);
			}
			blockFile.close();
		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("read file failed: " + e.getMessage());
			return resp;
		}
		return resp;
	}

	ProtocolResp handleWriteBlkReq(ConnectionInfo info, WriteFileReq req) {
		// TODO parameters validation
		WriteFileResp resp = new WriteFileResp(RespType.OK);
		DhtPath sourcePath = new DhtPath(req.getBlkName());
		if (!isAvailable(local, sourcePath)) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		DhtPath basePath = sourcePath.getMappingPath("_" + (req.getBaseBlkVersion()));
		DhtPath mappingPath = sourcePath.getMappingPath("_" + (req.getBaseBlkVersion() + 1));
		try {
			dataLockManager.acquireWriteLock(basePath.getAbsolutePath());
			String token = req.getToken();
			if (req.getToken() == null || req.getToken().trim().equals("")) {
				token = getToken(basePath, req.getBaseBlkVersion() + 1);
				if (req.getBaseBlkVersion() != 0) {
					dataFileSystem.copy(sourcePath.getMappingPath("_" + (req.getBaseBlkVersion())), mappingPath);
				} else {
					dataFileSystem.create(mappingPath).close();
				}
			} else {
				verifyToken(req.getToken(), basePath, req.getBaseBlkVersion() + 1);
			}
			IFile blockFile = dataFileSystem.open(mappingPath, IFile.WRITE);
			// blockFile.seek(req.getPos());
			blockFile.write(req.getBuf(), (int) req.getPos(), req.getLen());
			resp.setToken(token);
			resp.setFileSize(blockFile.length());
			resp.setCheckSum(blockFile.checkSum());
			blockFile.close();
		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("write file failed: " + e.getMessage());
			e.printStackTrace();
			return resp;
		} finally {
			dataLockManager.releaseWriteLock(basePath.getAbsolutePath());
		}
		return resp;
	}

	private void verifyToken(String token, DhtPath basePath, long version) throws IOException {
		DhtPath tokenPath = new DhtPath(
				basePath.getParentFile().getAbsolutePath() + "/" + token + ".token" + "-" + version);
		if (!dataFileSystem.exists(tokenPath)) {
			throw new IOException("token verification failed: " + token);
		}
	}

	private String getToken(DhtPath basePath, long version) throws IOException {
		DhtPath lockPath = new DhtPath(basePath.getAbsolutePath() + ".upgraded");
		if (dataFileSystem.exists(lockPath)) {
			throw new IOException("block has been overwritten by other");
		}
		dataFileSystem.create(lockPath).close();
		String token = tokenAssigner.generateUID();
		DhtPath tokenPath = new DhtPath(
				basePath.getParentFile().getAbsolutePath() + "/" + token + ".token" + "-" + version);
		dataFileSystem.create(tokenPath).close();
		return token;
	}

	// private DhtPath createTmpFile(String fileName) throws IOException {
	// File f = new File(conf.getProperty("tmpDir"));
	// return new DhtPath(File.createTempFile(fileName, ".tmp", f)
	// .getCanonicalPath());
	// }
	private FileMeta mergeVersion(FileMeta baseFileMeta, FileMeta newFileMeta) {
		FileMeta commitFileMeta = new FileMeta(newFileMeta.getFileName());
		int i = 0, j = 0, baseLen = baseFileMeta.getBlkNum(), newLen = newFileMeta.getBlkNum();
		System.out.println("baseLen: " + baseLen + " newLen: " + newLen);
		while (i < baseLen && !newFileMeta.getBlkNames().contains(baseFileMeta.getBlkNames().get(i))) {
			addBlk(commitFileMeta, baseFileMeta, i++);
		}
		while (j < newLen) {
			i = baseFileMeta.getBlkNames().indexOf(newFileMeta.getBlkNames().get(j));
			if (i != -1) {
				if (baseFileMeta.getBlkVersions().get(i) > newFileMeta.getBlkVersions().get(j)) {
					addBlk(commitFileMeta, baseFileMeta, i);
				} else {
					addBlk(commitFileMeta, newFileMeta, j);
				}
				++i;
				while (i < baseLen && !newFileMeta.getBlkNames().contains(baseFileMeta.getBlkNames().get(i))) {
					addBlk(commitFileMeta, baseFileMeta, i);
				}
			} else {
				addBlk(commitFileMeta, newFileMeta, j);
			}
			j++;
		}
		long fileSize = 0;
		int blkNum = commitFileMeta.getBlkNames().size();
		for (int num = 0; num < blkNum; ++num) {
			fileSize += commitFileMeta.getBlkSizes().get(num);
		}
		commitFileMeta.setBlkNum(blkNum);
		commitFileMeta.setFileSize(fileSize);
		return commitFileMeta;
	}

	private void addBlk(FileMeta commitFileMeta, FileMeta fileMeta, int idx) {
		commitFileMeta.getBlkVersions().add(fileMeta.getBlkVersions().get(idx));
		commitFileMeta.getBlkNames().add(fileMeta.getBlkNames().get(idx));
		commitFileMeta.getBlkSizes().add(fileMeta.getBlkSizes().get(idx));
		// commitFileMeta.getBlkCheckSums().add(
		// fileMeta.getBlkCheckSums().get(idx));
	}

	private boolean isAvailable(PhysicalNode local, DhtPath sourcePath) {
		return table.isAvailable(local, sourcePath);
	}
}
