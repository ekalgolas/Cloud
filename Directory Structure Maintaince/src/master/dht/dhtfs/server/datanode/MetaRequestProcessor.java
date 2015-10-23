package master.dht.dhtfs.server.datanode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import master.dht.dhtfs.core.CachedFileMeta;
import master.dht.dhtfs.core.DhtPath;
import master.dht.dhtfs.core.FileLockManager;
import master.dht.dhtfs.core.LocalMetaFileSystem;
import master.dht.dhtfs.core.def.IFile;
import master.dht.dhtfs.core.def.IFileSystem;
import master.dht.dhtfs.core.def.IIDAssigner;
import master.dht.dhtfs.core.def.ILockManager;
import master.dht.dhtfs.core.table.CachedRouteTable;
import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.dir.AddFileReq;
import master.dht.nio.protocol.dir.RemoveFileReq;
import master.dht.nio.protocol.meta.BlockLockReq;
import master.dht.nio.protocol.meta.BlockLockResp;
import master.dht.nio.protocol.meta.BlockNameReq;
import master.dht.nio.protocol.meta.BlockNameResp;
import master.dht.nio.protocol.meta.CommitFileReq;
import master.dht.nio.protocol.meta.CommitFileResp;
import master.dht.nio.protocol.meta.CreateFileReq;
import master.dht.nio.protocol.meta.CreateFileResp;
import master.dht.nio.protocol.meta.DeleteFileReq;
import master.dht.nio.protocol.meta.DeleteFileResp;
import master.dht.nio.protocol.meta.MetaUpdateReq;
import master.dht.nio.protocol.meta.MetaUpdateResp;
import master.dht.nio.protocol.meta.OpenFileReq;
import master.dht.nio.protocol.meta.OpenFileResp;
import master.dht.nio.server.ConnectionInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MetaRequestProcessor {

	Log log = LogFactory.getLog("meta");
	protected PhysicalNode local;
	protected CachedRouteTable cachedTable;
	protected IFileSystem metaFileSystem;
	protected ILockManager metaLockManager;
	protected IIDAssigner blockAssigner;
	protected CachedFileMeta cache;
	protected boolean metaCacheOpen;

	public MetaRequestProcessor(CachedRouteTable table, PhysicalNode loc)
			throws IOException {
		local = loc;
		cachedTable = table;
		metaFileSystem = new LocalMetaFileSystem();
		metaLockManager = new FileLockManager();
		blockAssigner = new BlockNameAssigner(loc.getUid(),
				DataServerConfiguration.getIdDir() + "/" + loc.getUid());
		cache = CachedFileMeta.getInstance();
		metaCacheOpen = DataServerConfiguration.isMetaCacheOpen();
	}

	public void initialize() throws IOException {
		metaFileSystem.initialize();
	}

	private String pathFilter(String path) {
		StringBuilder sb = new StringBuilder(path.replace('/', '_'));
		int cnt = (sb.length() - 1) / IFile.maxFileNameLen;
		while (cnt > 0) {
			sb.insert(cnt * IFile.maxFileNameLen, '/');
			--cnt;
		}
		return sb.toString();
	}

	ProtocolResp handleMetaUpdateReq(ConnectionInfo info, MetaUpdateReq req) {
		MetaUpdateResp resp = new MetaUpdateResp(RespType.OK);
		FileMeta fileMeta = req.getFileMeta();
		DhtPath sourcePath = new DhtPath(fileMeta.getFileName());
		if (!cachedTable.isAvailable(local, fileMeta.getFileName())) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		if (local.equals(cachedTable.getPrimary(fileMeta.getFileName()))) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("this is not the secondary meta server");
			return resp;
		}
		String folder = sourcePath.getMappingPath(
				DataServerConfiguration.getMetaDir()).getAbsolutePath()
				+ "/" + pathFilter(fileMeta.getFileName());
		String mappingPath = folder + "/meta";
		try {
			metaLockManager.acquireWriteLock(mappingPath);

			if (req.isCreate()) {
				IFile metaFile = metaFileSystem.create(mappingPath);
				byte[] data = fileMeta.toByteArray();
				metaFile.write(data);
				if (metaCacheOpen) {
					cache.update(mappingPath, data);
				}
				log.info("Type: Write FileName: "
						+ req.getFileMeta().getFileName() + " MetaSize: "
						+ metaFile.length());
				metaFile.close();

			} else if (!req.isDelete()) {
				IFile metaFile = metaFileSystem.open(mappingPath, IFile.WRITE);
				byte[] buf = null;
				if (metaCacheOpen) {
					buf = cache.get(mappingPath);
				}
				if (buf == null) {
					buf = new byte[(int) metaFile.length()];
					metaFile.read(buf);
					log.info("Type: Read FileName: " + fileMeta.getFileName()
							+ " MetaSize: " + metaFile.length());
				}
				FileMeta baseFileMeta = (FileMeta) FileMeta.fromBytes(buf);

				IFile backup = metaFileSystem.open(
						folder + "/" + baseFileMeta.getVersion(), IFile.WRITE);
				backup.write(buf);
				backup.close();

				fileMeta.setVersion(baseFileMeta.getVersion() + 1);
				metaFile.setLength(0);
				byte[] data = fileMeta.toByteArray();
				metaFile.write(data);
				log.info("Type: Write FileName: " + fileMeta.getFileName()
						+ " MetaSize: " + metaFile.length());
				metaFile.close();

				if (metaCacheOpen) {
					cache.update(mappingPath, data);
				}
			} else {
				if (metaCacheOpen) {
					cache.delete(mappingPath);
				}
				metaFileSystem.delete(folder);
			}
		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("meta update failed: " + e.getMessage());
			return resp;
		} finally {
			metaLockManager.releaseWriteLock(mappingPath);
		}
		return resp;
	}

	ProtocolResp handleCreateFileReq(ConnectionInfo info, CreateFileReq req) {
		CreateFileResp resp = new CreateFileResp(RespType.OK);
		DhtPath sourcePath = new DhtPath(req.getFileName());
		if (!cachedTable.isAvailable(local, req.getFileName())) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		if (!local.equals(cachedTable.getPrimary(req.getFileName()))) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("this is not the primary meta server");
			return resp;
		}
		String mappingPath = sourcePath.getMappingPath(
				DataServerConfiguration.getMetaDir()).getAbsolutePath()
				+ "/" + pathFilter(req.getFileName()) + "/meta";
		FileMeta fileMeta = new FileMeta(req.getFileName());
		try {
			metaLockManager.acquireWriteLock(mappingPath);
			IFile metaFile = metaFileSystem.create(mappingPath);
			byte[] data = fileMeta.toByteArray();
			metaFile.write(data);
			if (metaCacheOpen) {
				cache.update(mappingPath, data);
			}
			log.info("Type: Write FileName: " + req.getFileName()
					+ " MetaSize: " + metaFile.length());
			metaFile.close();

			MetaUpdateReq updateReq = new MetaUpdateReq(ReqType.META_UPDATE);
			updateReq.setFileMeta(fileMeta);
			updateReq.setDelete(false);
			updateReq.setCreate(true);
			forwardMetaToReplicas(req.getFileName(), updateReq);
			addIntoDir(req.getFileName(), 0);
		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("create meta file failed: " + e.getMessage());
			return resp;
		} finally {
			metaLockManager.releaseWriteLock(mappingPath);
		}
		return resp;
	}

	ProtocolResp handleOpenFileReq(ConnectionInfo info, OpenFileReq req) {
		OpenFileResp resp = new OpenFileResp(RespType.OK);
		DhtPath sourcePath = new DhtPath(req.getFileName());
		if (!cachedTable.isAvailable(local, req.getFileName())) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		String mappingPath = sourcePath.getMappingPath(
				DataServerConfiguration.getMetaDir()).getAbsolutePath()
				+ "/" + pathFilter(req.getFileName()) + "/meta";
		FileMeta fileMeta = null;
		try {
			metaLockManager.acquireReadLock(mappingPath);
			byte[] buf = null;
			if (metaCacheOpen) {
				buf = cache.get(mappingPath);
			}
			if (buf == null) {
				IFile metaFile = metaFileSystem.open(mappingPath, IFile.READ);
				buf = new byte[(int) metaFile.length()];
				metaFile.read(buf);
				log.info("Type: Read FileName: " + req.getFileName()
						+ " MetaSize: " + metaFile.length());
				metaFile.close();
				if (metaCacheOpen) {
					cache.update(mappingPath, buf);
				}
			}
			fileMeta = (FileMeta) FileMeta.fromBytes(buf);
		} catch (IOException e) {
			e.printStackTrace();
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("open meta file failed: " + e.getMessage());
			return resp;
		} finally {
			metaLockManager.releaseReadLock(mappingPath);
		}
		if (fileMeta == null) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("metaFile format error");
			return resp;
		}
		resp.setFileMeta(fileMeta);
		return resp;
	}

	ProtocolResp handleDeleteFileReq(ConnectionInfo info, DeleteFileReq req) {
		DeleteFileResp resp = new DeleteFileResp(RespType.OK);
		DhtPath sourcePath = new DhtPath(req.getFileName());
		if (!cachedTable.isAvailable(local, req.getFileName())) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		if (!local.equals(cachedTable.getPrimary(req.getFileName()))) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("this is not the primary meta server");
			return resp;
		}
		String folder = sourcePath.getMappingPath(
				DataServerConfiguration.getMetaDir()).getAbsolutePath()
				+ "/" + pathFilter(req.getFileName());
		String mappingPath = folder + "/meta";
		try {
			metaLockManager.acquireWriteLock(mappingPath);
			if (metaCacheOpen) {
				cache.delete(mappingPath);
			}
			metaFileSystem.delete(folder);
			MetaUpdateReq updateReq = new MetaUpdateReq(ReqType.META_UPDATE);
			updateReq.setFileMeta(new FileMeta(req.getFileName()));
			updateReq.setDelete(true);
			forwardMetaToReplicas(req.getFileName(), updateReq);
			removeFromDir(req.getFileName());
		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("delete meta file failed: " + e.getMessage());
			return resp;
		} finally {
			metaLockManager.releaseWriteLock(mappingPath);
		}
		return resp;
	}

	ProtocolResp handleCommitFileReq(ConnectionInfo info, CommitFileReq req) {
		CommitFileResp resp = new CommitFileResp(RespType.OK);
		FileMeta fileMeta = req.getFileMeta();
		DhtPath sourcePath = new DhtPath(fileMeta.getFileName());
		if (!cachedTable.isAvailable(local, fileMeta.getFileName())) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		if (!local.equals(cachedTable.getPrimary(fileMeta.getFileName()))) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("this is not the primary meta server");
			return resp;
		}
		String folder = sourcePath.getMappingPath(
				DataServerConfiguration.getMetaDir()).getAbsolutePath()
				+ "/" + pathFilter(fileMeta.getFileName());
		String mappingPath = folder + "/meta";
		long fileSize = 0;
		try {
			metaLockManager.acquireWriteLock(mappingPath);
			IFile metaFile = metaFileSystem.open(mappingPath, IFile.WRITE);
			byte[] buf = null;
			if (metaCacheOpen) {
				buf = cache.get(mappingPath);
			}
			if (buf == null) {
				buf = new byte[(int) metaFile.length()];
				metaFile.read(buf);
				log.info("Type: Read FileName: " + fileMeta.getFileName()
						+ " MetaSize: " + metaFile.length());
			}
			FileMeta baseFileMeta = (FileMeta) FileMeta.fromBytes(buf);

			IFile backup = metaFileSystem.open(
					folder + "/" + baseFileMeta.getVersion(), IFile.WRITE);
			backup.write(buf);
			backup.close();

			FileMeta mergedFileMeta = mergeVersion(baseFileMeta, fileMeta);
			mergedFileMeta.setVersion(baseFileMeta.getVersion() + 1);
			Set<String> locks = new HashSet<String>();
			for (String name : mergedFileMeta.getBlkLocks()) {
				locks.add(name);
			}
			for (int i = 0; i < req.getLocks().size(); ++i) {
				locks.remove(req.getLocks().get(i));
			}
			List<String> newLocks = new ArrayList<String>();
			for (String name : locks) {
				newLocks.add(name);
			}
			mergedFileMeta.setBlkLocks(newLocks);
			metaFile.setLength(0);
			byte[] data = mergedFileMeta.toByteArray();
			metaFile.write(data);
			log.info("Type: Write FileName: " + fileMeta.getFileName()
					+ " MetaSize: " + metaFile.length());
			metaFile.close();

			if (metaCacheOpen) {
				cache.update(mappingPath, data);
			}

			fileSize = mergedFileMeta.getFileSize();
			resp.setFileMeta(mergedFileMeta);

			MetaUpdateReq updateReq = new MetaUpdateReq(ReqType.META_UPDATE);
			updateReq.setFileMeta(mergedFileMeta);
			updateReq.setDelete(false);
			forwardMetaToReplicas(fileMeta.getFileName(), updateReq);
			addIntoDir(fileMeta.getFileName(), fileSize);
		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("commit meta file failed: " + e.getMessage());
			return resp;
		} finally {
			metaLockManager.releaseWriteLock(mappingPath);
		}
		return resp;
	}

	ProtocolResp handleNewBlockReq(ConnectionInfo info, BlockNameReq req) {
		BlockNameResp resp = new BlockNameResp(RespType.OK);
		List<Integer> newBlkSizes = new ArrayList<Integer>();
		List<String> newBlkNames = new ArrayList<String>();
		List<List<PhysicalNode>> newBlkServers = new ArrayList<List<PhysicalNode>>();
		long bytesToAdd = req.getBytesToAdd();
		int blkSize = req.getPreferredBlkSize();
		String blkName;
		while (bytesToAdd > 0) {
			try {
				blkName = blockAssigner.generateUID();
			} catch (IOException e) {
				resp.setResponseType(RespType.IOERROR);
				resp.setMsg("blkname assign failed: " + e.getMessage());
				return resp;
			}
			newBlkNames.add(blkName);
			newBlkSizes.add((int) Math.min(bytesToAdd, blkSize));

			List<PhysicalNode> nodes = cachedTable.getTable().randomNodes();
			newBlkServers.add(nodes);
			bytesToAdd -= blkSize;
		}
		resp.setNewBlkSizes(newBlkSizes);
		resp.setNewBlkNames(newBlkNames);
		resp.setNewBlkServers(newBlkServers);
		return resp;
	}

	ProtocolResp handleBlockLockReq(ConnectionInfo info, BlockLockReq req) {
		BlockLockResp resp = new BlockLockResp(RespType.OK);
		DhtPath sourcePath = new DhtPath(req.getFileName());
		if (!cachedTable.isAvailable(local, req.getFileName())) {
			resp.setResponseType(RespType.FWD);
			resp.setMsg("file is not in this server");
			return resp;
		}
		String folder = sourcePath.getMappingPath(
				DataServerConfiguration.getMetaDir()).getAbsolutePath()
				+ "/" + pathFilter(req.getFileName());
		String mappingPath = folder + "/meta";
		try {
			metaLockManager.acquireWriteLock(mappingPath);
			IFile metaFile = metaFileSystem.open(mappingPath, IFile.WRITE);
			byte[] buf = null;
			if (metaCacheOpen) {
				buf = cache.get(mappingPath);
			}
			if (buf == null) {
				buf = new byte[(int) metaFile.length()];
				metaFile.read(buf);
				log.info("Type: Read FileName: " + req.getFileName()
						+ " MetaSize: " + metaFile.length());
			}
			FileMeta fileMeta = (FileMeta) FileMeta.fromBytes(buf);
			Set<String> lockedBlks = new HashSet<String>();
			for (String name : fileMeta.getBlkLocks()) {
				lockedBlks.add(name);
			}
			List<String> locks = req.getBlkNames();
			for (int i = 0; i < locks.size(); ++i) {
				if (lockedBlks.contains(locks.get(i))) {
					resp.setResponseType(RespType.IOERROR);
					resp.setMsg("lock blocks failed");
					return resp;
				}
			}
			for (int i = 0; i < locks.size(); ++i) {
				fileMeta.getBlkLocks().add(locks.get(i));
			}
			metaFile.setLength(0);
			byte[] data = fileMeta.toByteArray();
			metaFile.write(data);
			log.info("Type: Write FileName: " + fileMeta.getFileName()
					+ " MetaSize: " + metaFile.length());
			metaFile.close();
			if (metaCacheOpen) {
				cache.update(mappingPath, data);
			}
			MetaUpdateReq updateReq = new MetaUpdateReq(ReqType.META_UPDATE);
			updateReq.setFileMeta(fileMeta);
			updateReq.setDelete(false);
			forwardMetaToReplicas(req.getFileName(), updateReq);
		} catch (IOException e) {
			resp.setResponseType(RespType.IOERROR);
			resp.setMsg("commit meta file failed: " + e.getMessage());
			return resp;
		} finally {
			metaLockManager.releaseWriteLock(mappingPath);
		}
		return resp;
	}

	private FileMeta mergeVersion(FileMeta baseFileMeta, FileMeta newFileMeta) {
		FileMeta commitFileMeta = new FileMeta(newFileMeta.getFileName());
		int i = 0, j = 0, baseLen = baseFileMeta.getBlkNum(), newLen = newFileMeta
				.getBlkNum();
		String baseBlkName, newBlkName;
		long baseBlkVersion, newBlkVersion;
		int blkSize;
		long fileSize = 0;
		while (i < baseLen && j < newLen) {
			baseBlkName = baseFileMeta.getBlkNames().get(i);
			newBlkName = newFileMeta.getBlkNames().get(j);
			if (!baseBlkName.equals(newBlkName)) {
				break;
			}
			baseBlkVersion = baseFileMeta.getBlkVersions().get(i);
			newBlkVersion = newFileMeta.getBlkVersions().get(j);
			blkSize = baseBlkVersion > newBlkVersion ? baseFileMeta
					.getBlkSizes().get(i) : newFileMeta.getBlkSizes().get(j);
			addBlk(commitFileMeta, Math.max(baseBlkVersion, newBlkVersion),
					blkSize, baseBlkName, null, baseFileMeta.getBlkServers()
							.get(i), baseFileMeta.getBlkLevels().get(i));
			fileSize += blkSize;
			i++;
			j++;
		}
		while (i < baseLen) {
			addBlk(commitFileMeta, baseFileMeta.getBlkVersions().get(i),
					baseFileMeta.getBlkSizes().get(i), baseFileMeta
							.getBlkNames().get(i), null, baseFileMeta
							.getBlkServers().get(i), baseFileMeta
							.getBlkLevels().get(i));
			fileSize += baseFileMeta.getBlkSizes().get(i);
			i++;
		}
		while (j < newLen) {
			addBlk(commitFileMeta, newFileMeta.getBlkVersions().get(j),
					newFileMeta.getBlkSizes().get(j), newFileMeta.getBlkNames()
							.get(j), null, newFileMeta.getBlkServers().get(j),
					newFileMeta.getBlkLevels().get(j));
			fileSize += newFileMeta.getBlkSizes().get(j);
			j++;
		}
		commitFileMeta.setFileSize(fileSize);
		commitFileMeta.setBlkNum(commitFileMeta.getBlkNames().size());
		return commitFileMeta;
	}

	private void addBlk(FileMeta commitFileMeta, long blkVersion, int blkSize,
			String blkName, String blkCheckSum, List<PhysicalNode> blkServer,
			List<Integer> blkLevels) {
		commitFileMeta.getBlkNames().add(blkName);
		commitFileMeta.getBlkVersions().add(blkVersion);
		commitFileMeta.getBlkSizes().add(blkSize);
		commitFileMeta.getBlkServers().add(blkServer);
		commitFileMeta.getBlkCheckSums().add(blkCheckSum);
		commitFileMeta.getBlkLevels().add(blkLevels);
	}

	// private FileMeta mergeVersion(FileMeta baseFileMeta, FileMeta
	// newFileMeta) {
	// FileMeta commitFileMeta = new FileMeta(newFileMeta.getFileName());
	// int i = 0, j = 0, baseLen = baseFileMeta.getBlkNum(), newLen =
	// newFileMeta
	// .getBlkNum();
	// // System.out.println("baseLen: " + baseLen + " newLen: " + newLen);
	// while (i < baseLen
	// && !newFileMeta.getBlkNames().contains(
	// baseFileMeta.getBlkNames().get(i))) {
	// addBlk(commitFileMeta, baseFileMeta, i++);
	// }
	// while (j < newLen) {
	// i = baseFileMeta.getBlkNames().indexOf(
	// newFileMeta.getBlkNames().get(j));
	// if (i != -1) {
	// if (baseFileMeta.getBlkVersions().get(i) > newFileMeta
	// .getBlkVersions().get(j)) {
	// addBlk(commitFileMeta, baseFileMeta, i);
	// } else {
	// addBlk(commitFileMeta, newFileMeta, j);
	// }
	// ++i;
	// while (i < baseLen
	// && !newFileMeta.getBlkNames().contains(
	// baseFileMeta.getBlkNames().get(i))) {
	// addBlk(commitFileMeta, baseFileMeta, i);
	// }
	// } else {
	// addBlk(commitFileMeta, newFileMeta, j);
	// }
	// j++;
	// }
	// long fileSize = 0;
	// int blkNum = commitFileMeta.getBlkNames().size();
	// for (int num = 0; num < blkNum; ++num) {
	// fileSize += commitFileMeta.getBlkSizes().get(num);
	// }
	// commitFileMeta.setBlkNum(blkNum);
	// commitFileMeta.setFileSize(fileSize);
	// return commitFileMeta;
	// }
	//
	// private void addBlk(FileMeta commitFileMeta, FileMeta fileMeta, int idx)
	// {
	// commitFileMeta.getBlkVersions().add(fileMeta.getBlkVersions().get(idx));
	// commitFileMeta.getBlkNames().add(fileMeta.getBlkNames().get(idx));
	// commitFileMeta.getBlkSizes().add(fileMeta.getBlkSizes().get(idx));
	// // commitFileMeta.getBlkCheckSums().add(
	// // fileMeta.getBlkCheckSums().get(idx));
	// }

	private void addIntoDir(String fileName, long fileSize) throws IOException {
		if (local.equals(cachedTable.getPrimary(fileName))) {
			DhtPath path = new DhtPath(fileName);
			String dirKey = path.getDirKey();
			PhysicalNode node = selectDirServer(dirKey);
			AddFileReq req = new AddFileReq(ReqType.DIR_ADD_FILE);
			FileInfo info = new FileInfo();
			info.setFile(true);
			info.setFileName(fileName);
			info.setFileSize(fileSize);
			req.setFileInfo(info);
			req.setDirKey(dirKey);
			TCPConnection con = TCPConnection.getInstance(node.getIpAddress(),
					node.getPort());
			con.request(req);
			ProtocolResp resp = con.response();
			con.close();
			if (resp.getResponseType() != RespType.OK) {
				throw new IOException("addIntoDir failed: " + resp.getMsg());
			}
		}
	}

	private void removeFromDir(String fileName) throws IOException {
		if (local.equals(cachedTable.getPrimary(fileName))) {
			DhtPath path = new DhtPath(fileName);
			String dirKey = path.getDirKey();
			PhysicalNode node = selectDirServer(dirKey);
			RemoveFileReq req = new RemoveFileReq(ReqType.DIR_DELETE_FILE);
			req.setFileName(fileName);
			req.setDirKey(dirKey);
			TCPConnection con = TCPConnection.getInstance(node.getIpAddress(),
					node.getPort());
			con.request(req);
			ProtocolResp resp = con.response();
			con.close();
			if (resp.getResponseType() != RespType.OK) {
				throw new IOException("removeFromDir failed: " + resp.getMsg());
			}
		}
	}

	private void forwardMetaToReplicas(String fileName, ProtocolReq req)
			throws IOException {
		if (local.equals(cachedTable.getPrimary(fileName))) {
			List<PhysicalNode> nodes = cachedTable.getPhysicalNodes(fileName);
			List<TCPConnection> connections = new ArrayList<TCPConnection>();
			for (int i = 0; i < nodes.size(); ++i) {
				if (!nodes.get(i).equals(local)) {
					TCPConnection con = TCPConnection.getInstance(nodes.get(i)
							.getIpAddress(), nodes.get(i).getPort());
					connections.add(con);
					con.request(req);
				}
			}
			boolean error = false;
			StringBuilder msg = new StringBuilder();
			for (int i = 0; i < connections.size(); ++i) {
				ProtocolResp replicaResp = connections.get(i).response();
				connections.get(i).close();
				if (replicaResp.getResponseType() != RespType.OK) {
					error = true;
					msg.append(replicaResp.getMsg() + "\n");
				}
			}
			if (error) {
				throw new IOException("meta replica write failed: " + msg);
			}
		}
	}

	private PhysicalNode selectDirServer(String dirKey) {
		return cachedTable.getPrimary(dirKey);
	}

}
