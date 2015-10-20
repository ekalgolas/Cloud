package master.dht.dhtfs.server.datanode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import master.dht.dhtfs.core.FileLockManager;
import master.dht.dhtfs.core.LocalBlockFileSystem;
import master.dht.dhtfs.core.def.IFile;
import master.dht.dhtfs.core.def.IFileSystem;
import master.dht.dhtfs.core.def.ILockManager;
import master.dht.dhtfs.core.table.CachedRouteTable;
import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.block.ReadFileReq;
import master.dht.nio.protocol.block.ReadFileResp;
import master.dht.nio.protocol.block.WriteFileReq;
import master.dht.nio.protocol.block.WriteFileResp;
import master.dht.nio.protocol.block.WriteFinishReq;
import master.dht.nio.protocol.block.WriteFinishResp;
import master.dht.nio.protocol.block.WriteInitReq;
import master.dht.nio.protocol.block.WriteInitResp;
import master.dht.nio.server.ConnectionInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BlockRequestProcessor {

    Log log = LogFactory.getLog("block");
    protected PhysicalNode local;
    protected CachedRouteTable cachedTable;
    protected IFileSystem dataFileSystem;
    protected ILockManager dataLockManager;
    protected TransactionManager transactionManager;
    protected AsynDataReplicator replicator;
    protected LocalPathManager pathManager;

    public BlockRequestProcessor(CachedRouteTable table, PhysicalNode loc) {
        local = loc;
        cachedTable = table;
        dataFileSystem = new LocalBlockFileSystem();
        dataLockManager = new FileLockManager();
        transactionManager = new TransactionManager();
        replicator = new AsynDataReplicator(transactionManager);
        pathManager = new LocalPathManager(
                DataServerConfiguration.getDataDir(),
                DataServerConfiguration.getFilePerDir(),
                DataServerConfiguration.getDirPerLevel());
    }

    public void initialize() throws IOException {
        dataFileSystem.initialize();
        replicator.initialize();
        pathManager.initialize();
    }

    ProtocolResp handleReadBlkReq(ConnectionInfo info, ReadFileReq req) {
        ReadFileResp resp = new ReadFileResp(RespType.OK);
        String mappingPath = pathManager.getMappedPath(req.getBlkName(),
                req.getLevel())
                + IFile.delim + req.getBlkVersion();
        try {
            IFile blockFile = dataFileSystem.open(mappingPath);
            byte[] buf = new byte[req.getLen()];
            blockFile.seek(req.getPos());
            blockFile.read(buf);
            resp.setBuf(buf);
            resp.setPos(req.getPos());
            blockFile.close();
        } catch (IOException e) {
            resp.setResponseType(RespType.IOERROR);
            resp.setMsg("read file failed: " + e.getMessage());
            return resp;
        }
        log.info("Type: Read BlockName: " + req.getBlkName()
                + " BlockVersion: " + req.getBlkVersion() + " Pos: "
                + req.getPos() + " Len: " + req.getLen() + " Level: "
                + req.getLevel());
        return resp;
    }

    ProtocolResp handleWriteInitReq(ConnectionInfo info, WriteInitReq req) {
        WriteInitResp resp = new WriteInitResp(RespType.OK);
        WriteFileTransaction transaction = new WriteFileTransaction(
                TransactionType.WriteFile);

        try {
            transaction.setBlkName(req.getBlkName());

            String mappingPath;
            int level = req.getLevel();
            if (req.isBaseVersion()) {
                transaction.setBlkVersion(req.getBlkVersion() + 1);
                if (req.getBlkVersion() <= 0) {
                    level = pathManager.assignLevel();
                }
                String dir = pathManager.getMappedPath(req.getBlkName(), level)
                        + IFile.delim;
                String basePath = dir + req.getBlkVersion();
                mappingPath = dir + (req.getBlkVersion() + 1);
                if (req.getBlkVersion() > 0) {
                    dataFileSystem.copy(basePath, mappingPath);
                } else {
                    dataFileSystem.create(mappingPath).close();
                }
            } else {
                transaction.setBlkVersion(req.getBlkVersion());
                mappingPath = pathManager.getMappedPath(req.getBlkName(),
                        req.getLevel())
                        + IFile.delim + req.getBlkVersion();
            }
            transaction.setLevel(level);
            IFile file = dataFileSystem.open(mappingPath, IFile.WRITE);
            transaction.setFile(file);
            transaction.setPos(req.getPos());
            transaction.setLen(req.getLen());
            transaction.setInsert(req.isInsert());

            List<PhysicalNode> replicas = req.getReplicas();
            List<Integer> replicaLevels = req.getReplicaLevels();
            if (!replicas.isEmpty()) {
                transaction.setReplica(replicas.get(0));
                transaction.setReplicaLevel(replicaLevels.get(0));
            }
            List<PhysicalNode> nextReplicas = new ArrayList<PhysicalNode>();
            List<Integer> nextReplicaLevels = new ArrayList<Integer>();
            for (int i = 1; i < replicas.size(); ++i) {
                nextReplicas.add(replicas.get(i));
                nextReplicaLevels.add(replicaLevels.get(i));
            }
            transaction.setReplicas(nextReplicas);
            transaction.setReplicaLevels(nextReplicaLevels);
            transaction.setConnectionInfo(info);

            String id = transactionManager.addTransaction(transaction);

            if (transaction.getReplica() != null) {
                req.setLevel(transaction.getReplicaLevel());
                req.setReplicas(transaction.getReplicas());
                req.setReplicaLevels(transaction.getReplicaLevels());
                replicator.register(id);
                replicator.addReq(id, req);
            }

            resp.setBlkVersion(transaction.getBlkVersion());
            resp.setTransactionId(id);

        } catch (IOException e) {
            resp.setResponseType(RespType.IOERROR);
            resp.setMsg("write init failed: " + e.getMessage());
            e.printStackTrace();
            return resp;
        }
        return resp;
    }

    ProtocolResp handleWriteFinishReq(ConnectionInfo info, WriteFinishReq req) {
        // WriteFileTransaction transaction = (WriteFileTransaction)
        // transactionManager
        // .getTransaction(req.getTransactionId());
        WriteFileTransaction transaction = (WriteFileTransaction) transactionManager
                .getTransaction(req.getTransactionId());

        if (transaction.getReplica() != null) {
            replicator.addReq(req.getTransactionId(), req);
            return null;
        } else {
            WriteFinishResp resp = new WriteFinishResp(RespType.OK);
            List<Integer> levels = new ArrayList<Integer>();
            levels.add(transaction.getLevel());
            resp.setLevels(levels);
            return resp;
        }
        // if (transaction.isDone()) {
        // WriteFinishResp resp = new WriteFinishResp(RespType.OK);
        // try {
        // transaction.release();
        // } catch (IOException e) {
        // resp.setResponseType(RespType.IOERROR);
        // resp.setMsg("write finish failed: " + e.getMessage());
        // e.printStackTrace();
        // return resp;
        // }
        // return resp;
        // }
    }

    ProtocolResp handleWriteBlkReq(ConnectionInfo info, WriteFileReq req) {
        WriteFileResp resp = new WriteFileResp(RespType.OK);
        WriteFileTransaction transaction = (WriteFileTransaction) transactionManager
                .getTransaction(req.getTransactionId());
        IFile blockFile = transaction.getFile();
        synchronized (blockFile) {
            try {
//                System.out.println(req.getPos() + " ***** " + req.getLen());
                blockFile.seek(req.getPos());
                if (transaction.isInsert()) {
                    blockFile.insert(req.getBuf(), 0, req.getBuf().length);
                } else {
                    blockFile.write(req.getBuf(), 0, req.getBuf().length);
                }
            } catch (IOException e) {
                resp.setResponseType(RespType.IOERROR);
                resp.setMsg("write file failed: " + e.getMessage());
                e.printStackTrace();
                return resp;
            }
        }
        req.setBuf(null);
        if (transaction.getReplica() != null) {
            replicator.addReq(req.getTransactionId(), req);
        }
        // List<PhysicalNode> replicas = req.getReplicas();
        // if (!req.getReplicas().isEmpty()) {
        // TCPConnection con = TCPConnection.getInstance(replicas.get(0)
        // .getIpAddress(), replicas.get(0).getPort());
        // List<PhysicalNode> nextReplicas = new ArrayList<PhysicalNode>();
        // for (int i = 1; i < replicas.size(); ++i) {
        // nextReplicas.add(replicas.get(i));
        // }
        // req.setReplicas(nextReplicas);
        // con.request(req);
        // WriteFileResp replicaResp = (WriteFileResp) con.response();
        // con.close();
        // if (replicaResp.getResponseType() != RespType.OK) {
        // return replicaResp;
        // }
        // }
        // } catch (IOException e) {
        // resp.setResponseType(RespType.IOERROR);
        // resp.setMsg("write file failed: " + e.getMessage());
        // e.printStackTrace();
        // return resp;
        // }
        log.info("Type: Write BlockName: " + transaction.getBlkName()
                + " BlockVersion: " + transaction.getBlkVersion() + " Pos: "
                + req.getPos() + " Len: " + req.getLen() + " Level: "
                + transaction.getLevel());
        return resp;
    }
}
