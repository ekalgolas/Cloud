package master.dht.dhtfs.server.datanode;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import master.dht.dhtfs.core.def.IFile;
import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.server.ConnectionInfo;

public class WriteFileTransaction extends Transaction {

    private String blkName;
    private long blkVersion;
    private long pos;
    private int len;
    private boolean isInsert;
    private int level;

    private ConnectionInfo connectionInfo;
    private SelectionKey key;
    private PhysicalNode replica;
    private int replicaLevel;
    private List<PhysicalNode> replicas;
    private List<Integer> replicaLevels;

    private IOBuffer<ProtocolResp, ProtocolReq> ioBuffer;

    private IFile file;
    private String replicaTransactionId;

    private AtomicInteger replicaBytesWritten;
    private boolean canWrite;

    // private Queue<ProtocolReq> reqQueue;

    public WriteFileTransaction(TransactionType transactionType) {
        super(transactionType);
        replicaBytesWritten = new AtomicInteger(0);
        setCanWrite(true);
        // reqQueue = new ConcurrentLinkedQueue<ProtocolReq>();
    }

    public boolean isDone() {
        return replica == null || replicaBytesWritten.get() == len;
    }

    public void replicaWrite(int val) {
        int size = replicaBytesWritten.addAndGet(val);
        if (size == len) {
            synchronized (this) {
                notify();
            }
        }
    }

    public String getBlkName() {
        return blkName;
    }

    public void setBlkName(String blkName) {
        this.blkName = blkName;
    }

    public long getBlkVersion() {
        return blkVersion;
    }

    public void setBlkVersion(long blkVersion) {
        this.blkVersion = blkVersion;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public boolean isInsert() {
        return isInsert;
    }

    public void setInsert(boolean isInsert) {
        this.isInsert = isInsert;
    }

    public List<PhysicalNode> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<PhysicalNode> replicas) {
        this.replicas = replicas;
    }

    public IFile getFile() {
        return file;
    }

    public void setFile(IFile file) {
        this.file = file;
    }

    public void release() throws IOException {
        if (file != null) {
            file.close();
        }
        if (key != null) {
            key.cancel();
            key.channel().close();
        }
    }

    public String getReplicaTransactionId() {
        return replicaTransactionId;
    }

    public void setReplicaTransactionId(String replicaTransactionId) {
        this.replicaTransactionId = replicaTransactionId;
    }

    public PhysicalNode getReplica() {
        return replica;
    }

    public void setReplica(PhysicalNode replica) {
        this.replica = replica;
    }

    public SelectionKey getKey() {
        return key;
    }

    public void setKey(SelectionKey key) {
        this.key = key;
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public void setConnectionInfo(ConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
    }

    public IOBuffer<ProtocolResp, ProtocolReq> getIoBuffer() {
        return ioBuffer;
    }

    public void setIoBuffer(IOBuffer<ProtocolResp, ProtocolReq> ioBuffer) {
        this.ioBuffer = ioBuffer;
    }

    public boolean isCanWrite() {
        return canWrite;
    }

    public void setCanWrite(boolean canWrite) {
        this.canWrite = canWrite;
    }

    public List<Integer> getReplicaLevels() {
        return replicaLevels;
    }

    public void setReplicaLevels(List<Integer> replicaLevels) {
        this.replicaLevels = replicaLevels;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getReplicaLevel() {
        return replicaLevel;
    }

    public void setReplicaLevel(int replicaLevel) {
        this.replicaLevel = replicaLevel;
    }

}
