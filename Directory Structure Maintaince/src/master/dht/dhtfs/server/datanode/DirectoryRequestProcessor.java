package master.dht.dhtfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import master.dht.dhtfs.core.DhtPath;
import master.dht.dhtfs.core.table.CachedRouteTable;
import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.dir.AddFileReq;
import master.dht.nio.protocol.dir.AddFileResp;
import master.dht.nio.protocol.dir.ListStatusReq;
import master.dht.nio.protocol.dir.ListStatusResp;
import master.dht.nio.protocol.dir.RemoveFileReq;
import master.dht.nio.protocol.dir.RemoveFileResp;
import master.dht.nio.server.ConnectionInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DirectoryRequestProcessor {

    Log log = LogFactory.getLog("dir");
    protected PhysicalNode local;
    protected CachedRouteTable cachedTable;
    protected DirMeta dir;
    protected DirMetaHistory dirHistory;

    public DirectoryRequestProcessor(CachedRouteTable table, PhysicalNode loc) {
        local = loc;
        cachedTable = table;
    }

    public void initialize() throws IOException {
        dir = DirMeta.getInstance();
        try {
            DirMetaHistory.recovery(dir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        dirHistory = DirMetaHistory.getInstance();
    }

    ProtocolResp handleAddFile(ConnectionInfo info, AddFileReq req) {
        AddFileResp resp = new AddFileResp(RespType.OK);
        FileInfo fileInfo = req.getFileInfo();
        DhtPath path = new DhtPath(req.getDirKey());
        String parentDirKey = path.getParentKey();
        try {
            addIntoParentNode(parentDirKey, req.getDirKey(), req.getFileInfo());
            forwardDirToReplicas(req.getDirKey(), req);
            dirHistory.pushAddHistory(fileInfo);
            dir.addFile(fileInfo);
        } catch (IOException e) {
            resp.setResponseType(RespType.IOERROR);
            resp.setMsg("add file to dir failed: " + e.getMessage());
            return resp;
        }
        return resp;
    }

    ProtocolResp handleRemoveFile(ConnectionInfo info, RemoveFileReq req) {
        RemoveFileResp resp = new RemoveFileResp(RespType.OK);
        DhtPath path = new DhtPath(req.getDirKey());
        String parentDirKey = path.getParentKey();
        try {
            removeFromParentNode(parentDirKey, req.getDirKey(),
                    req.getFileName());
            forwardDirToReplicas(req.getDirKey(), req);
            FileInfo fileInfo = new FileInfo(req.getFileName());
            dirHistory.pushDeleteHistory(fileInfo);
            dir.removeFile(fileInfo);
        } catch (IOException e) {
            resp.setResponseType(RespType.IOERROR);
            resp.setMsg("remove file failed: " + e.getMessage());
            return resp;
        }
        return resp;
    }

    ProtocolResp handleListStatus(ConnectionInfo info, ListStatusReq req) {
        ListStatusResp resp = new ListStatusResp(RespType.OK);
        resp.setFileInfo(dir.listStatus(req.getFileName()));
        return resp;
    }

    private void forwardDirToReplicas(String dirKey, ProtocolReq req)
            throws IOException {
        if (local.equals(cachedTable.getPrimary(dirKey))) {
            List<PhysicalNode> nodes = cachedTable.getPhysicalNodes(dirKey);
            List<TCPConnection> connections = new ArrayList<TCPConnection>();
            for (int i = 0; i < nodes.size(); ++i) {
                if (!nodes.get(i).equals(local)) {
                    TCPConnection con = TCPConnection.getInstance(nodes.get(i)
                            .getIpAddress(), nodes.get(i).getPort());
                    log.info("Type: Connect-to-replica DirKey: " + dirKey);
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
                throw new IOException("dir replica write failed: " + msg);
            }
        }
    }

    private void addIntoParentNode(String parentDirKey, String dirKey,
            FileInfo fileInfo) throws IOException {
        if (parentDirKey == null || new File(dirKey).exists()) {
            return;
        }
        if (local.equals(cachedTable.getPrimary(dirKey))) {
            FileInfo parentFileInfo = new FileInfo();
            parentFileInfo.setFileName(dirKey);
            parentFileInfo.setFile(false);
            if (dirKey.equals(fileInfo.getFileName())) {
                parentFileInfo.setFile(fileInfo.isFile());
                parentFileInfo.setFileSize(fileInfo.getFileSize());
            }

            AddFileReq req = new AddFileReq(ReqType.DIR_ADD_FILE);
            req.setFileInfo(parentFileInfo);
            req.setDirKey(parentDirKey);

            PhysicalNode node = cachedTable.getPrimary(parentDirKey);
            TCPConnection connection = TCPConnection.getInstance(
                    node.getIpAddress(), node.getPort());
            log.info("Type: Connect-to-parent-add ParentKey: " + parentDirKey);
            connection.request(req);
            ProtocolResp resp = connection.response();
            connection.close();
            if (resp.getResponseType() != RespType.OK) {
                throw new IOException("addIntoParentNode failed: "
                        + resp.getMsg());
            }
        }
    }

    private void removeFromParentNode(String parentDirKey, String dirKey,
            String fileName) throws IOException {
        if (parentDirKey == null || !dirKey.equals(fileName)) {
            return;
        }

        if (local.equals(cachedTable.getPrimary(dirKey))) {
            RemoveFileReq req = new RemoveFileReq(ReqType.DIR_DELETE_FILE);
            req.setFileName(dirKey);
            req.setDirKey(parentDirKey);

            PhysicalNode node = cachedTable.getPrimary(parentDirKey);
            TCPConnection connection = TCPConnection.getInstance(
                    node.getIpAddress(), node.getPort());
            log.info("Type: Connect-to-parent-remove ParentKey: "
                    + parentDirKey);
            connection.request(req);
            ProtocolResp resp = connection.response();
            connection.close();
            if (resp.getResponseType() != RespType.OK) {
                throw new IOException("addIntoParentNode failed: "
                        + resp.getMsg());
            }
        }
    }

}
