package master.dht.dhtfs.client;

import java.io.IOException;

import master.dht.dhtfs.core.DhtPath;
import master.dht.dhtfs.core.GeometryLocation;
import master.dht.dhtfs.core.def.IFile;
import master.dht.dhtfs.core.def.IFileSystem;
import master.dht.dhtfs.core.table.CachedRouteTable;
import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.dhtfs.server.datanode.FileInfo;
import master.dht.dhtfs.server.datanode.FileMeta;
import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.dir.AddFileReq;
import master.dht.nio.protocol.dir.ListStatusReq;
import master.dht.nio.protocol.dir.ListStatusResp;
import master.dht.nio.protocol.dir.RemoveFileReq;
import master.dht.nio.protocol.meta.CreateFileReq;
import master.dht.nio.protocol.meta.CreateFileResp;
import master.dht.nio.protocol.meta.DeleteFileReq;
import master.dht.nio.protocol.meta.DeleteFileResp;
import master.dht.nio.protocol.meta.OpenFileReq;
import master.dht.nio.protocol.meta.OpenFileResp;

public class DHTFileSystem implements IFileSystem {

    protected PhysicalNode master;
    protected CachedRouteTable table;
    protected GeometryLocation location;

    @Override
    public void initialize() throws IOException {
        master = new PhysicalNode(ClientConfiguration.getMasterIp(),
                ClientConfiguration.getMasterPort());
        location = new GeometryLocation(ClientConfiguration.getLatitude(),
                ClientConfiguration.getLongitude());
        table = new CachedRouteTable(master);
        table.updateRouteTable();
    }

    @Override
    public DHTFile create(String path) throws IOException {
        PhysicalNode metaServer = selectMetaServer(path);
        TCPConnection connection = TCPConnection.getInstance(
                metaServer.getIpAddress(), metaServer.getPort());
        CreateFileReq req = new CreateFileReq(ReqType.CREATE_FILE);
        req.setFileName(path);
        req.setBytesToAdd(0);
        req.setPreferredBlkSize(0);
        connection.request(req);
        System.out.println("create file request sent: "
                + metaServer.getIpAddress() + ":" + metaServer.getPort());
        CreateFileResp resp = (CreateFileResp) connection.response();
//        System.out.println("create file response");
        if (resp.getResponseType() != RespType.OK) {
            throw new IOException("create file " + path + " failed, error: "
                    + resp.getResponseType() + " msg: " + resp.getMsg());
        }
//        System.out.println("create file succeed");
        FileMeta fileMeta = new FileMeta(path);
        return new DHTFile(IFile.CREATE | IFile.READ | IFile.WRITE
                | IFile.APPEND, metaServer, fileMeta, resp.getNewBlkSizes(),
                resp.getNewBlkNames(), resp.getNewBlkServers());
    }

    @Override
    public DHTFile open(String path) throws IOException {
        return open(path, IFile.READ);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IFileSystem#open(master.dht.dhtfs.core.String, int)
     */
    @Override
    public DHTFile open(String path, int mod) throws IOException {
        PhysicalNode metaServer = selectMetaServer(path);
        TCPConnection connection = TCPConnection.getInstance(
                metaServer.getIpAddress(), metaServer.getPort());
        OpenFileReq req = new OpenFileReq(ReqType.OPEN_FILE);
        req.setFileName(path);
        req.setBytesToAdd(0);
        req.setPreferredBlkSize(0);
        if ((mod & IFile.WRITE) != 0) {
            req.setWriteLock(true);
        }
        if ((mod & IFile.APPEND) != 0) {
            req.setAppendLock(true);
        }
        connection.request(req);
        OpenFileResp resp = (OpenFileResp) connection.response();
        if (resp.getResponseType() != RespType.OK) {
            throw new IOException("open file " + path + " failed, error: "
                    + resp.getResponseType() + " msg: " + resp.getMsg());
        }
        FileMeta fileMeta = resp.getFileMeta();
        return new DHTFile(mod, metaServer, fileMeta, resp.getNewBlkSizes(),
                resp.getNewBlkNames(), resp.getNewBlkServers());
    }

    @Override
    public void delete(String path) throws IOException {
        PhysicalNode metaServer = selectMetaServer(path);
        TCPConnection connection = TCPConnection.getInstance(
                metaServer.getIpAddress(), metaServer.getPort());
        DeleteFileReq req = new DeleteFileReq(ReqType.DELETE_FILE);
        req.setFileName(path);
        connection.request(req);
        DeleteFileResp resp = (DeleteFileResp) connection.response();
        if (resp.getResponseType() != RespType.OK) {
            throw new IOException("delete file " + path + " failed, error: "
                    + resp.getResponseType() + " msg: " + resp.getMsg());
        }
    }

    @Override
    public void copyFromLocal(String srcPath, String dstPath)
            throws IOException {
        DHTFile file = create(dstPath);
        file.upload(srcPath);
        file.commit();
        file.close();
    }

    @Override
    public void copyToLocal(String srcPath, String dstPath) throws IOException {
        DHTFile file = open(srcPath, IFile.READ);
        file.download(dstPath);
        file.close();
    }

    @Override
    public void rename(String srcPath, String dstPath) throws IOException {
        // TODO Auto-generated method stub
        throw new IOException("have not implemented yet");
    }

    @Override
    public void copy(String srcPath, String dstPath) throws IOException {
        // TODO Auto-generated method stub
        throw new IOException("have not implemented yet");
    }

    @Override
    public void mkdir(String fileName) throws IOException {
        DhtPath path = new DhtPath(fileName);
        String dirKey = path.getDirKey();
        PhysicalNode node = selectDirServer(dirKey);
        AddFileReq req = new AddFileReq(ReqType.DIR_ADD_FILE);
        FileInfo info = new FileInfo();
        info.setFile(false);
        info.setFileName(fileName);
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

    @Override
    public void rmdir(String fileName, boolean recursive) throws IOException {
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

    @Override
    public void listStatus(String fileName) throws IOException {
        DhtPath path = new DhtPath(fileName);
        String dirKey = path.getDirKey();
        PhysicalNode dirServer = selectDirServer(dirKey);
        TCPConnection connection = TCPConnection.getInstance(
                dirServer.getIpAddress(), dirServer.getPort());
        ListStatusReq req = new ListStatusReq(ReqType.DIR_LIST_STATUS);
        req.setFileName(path.getAbsolutePath());
        connection.request(req);
        ListStatusResp resp = (ListStatusResp) connection.response();
        if (resp.getResponseType() != RespType.OK) {
            throw new IOException("list file status failed: " + path
                    + ", error: " + resp.getResponseType() + " msg: "
                    + resp.getMsg());
        }
        FileInfo fileInfo = resp.getFileInfo();
        if (fileInfo.isFile()) {
//            System.out.println("FileName: " + fileInfo.getFileName()
//                    + " FileSize: " + fileInfo.getFileSize());
        } else {
            for (FileInfo subFile : fileInfo.getFileInfos()) {
                if (!subFile.isFile()) {
//                    System.out.println("DirName: " + subFile.getFileName());
                } else {
					// System.out.println("FileName: " + subFile.getFileName()
					//   + " FileSize: " + subFile.getFileSize());
                }
            }
        }
    }

    @Override
    public boolean isDirectory(String path) throws IOException {
        // TODO Auto-generated method stub
        throw new IOException("have not implemented yet");
    }

    @Override
    public boolean isFile(String path) throws IOException {
        // TODO Auto-generated method stub
        throw new IOException("have not implemented yet");
    }

    @Override
    public boolean exists(String path) throws IOException {
        // TODO Auto-generated method stub
        throw new IOException("have not implemented yet");
    }

    private PhysicalNode selectMetaServer(String path) {
        // return table.getNearestNode(path, location);
        return table.getPrimary(path);
    }

    private PhysicalNode selectDirServer(String dirKey) {
        return table.getPrimary(dirKey);
    }

}
