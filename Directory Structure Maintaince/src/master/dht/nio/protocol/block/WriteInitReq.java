package master.dht.nio.protocol.block;

import java.util.List;

import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class WriteInitReq extends ProtocolReq {
    private static final long serialVersionUID = 1L;

    private String blkName;
    private long blkVersion;
    private boolean isBaseVersion;
    private long pos;
    private int len;
    private boolean isInsert;
    private int level;

    private List<Integer> replicaLevels;
    private List<PhysicalNode> replicas;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("blkName: " + blkName);
        System.out.println("blkVersion: " + blkVersion);
        System.out.println("isBaseVersion: " + isBaseVersion);
        System.out.println("pos: " + pos);
        System.out.println("len: " + len);
        System.out.println("isInsert: " + isInsert);
        System.out.println("***********END***********");
    }

    public WriteInitReq(ReqType requestType) {
        super(requestType);
        setInsert(false);
    }

    public WriteInitReq(int rId, ReqType requestType) {
        super(rId, requestType);
        setInsert(false);
    }

    public boolean isBaseVersion() {
        return isBaseVersion;
    }

    public void setBaseVersion(boolean isBaseVersion) {
        this.isBaseVersion = isBaseVersion;
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

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
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

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public List<Integer> getReplicaLevels() {
        return replicaLevels;
    }

    public void setReplicaLevels(List<Integer> replicaLevels) {
        this.replicaLevels = replicaLevels;
    }

}
