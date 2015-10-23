package master.dht.nio.protocol.meta;

import java.util.List;

import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.dhtfs.server.datanode.FileMeta;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class OpenFileResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    private FileMeta fileMeta;

    private List<Integer> newBlkSizes;
    private List<String> newBlkNames;
    private List<List<PhysicalNode>> newBlkServers;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("fileMeta: " + fileMeta.toString());
        System.out.println("***********END***********");
    }

    public OpenFileResp(RespType responseType) {
        super(responseType);
    }

    public OpenFileResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public FileMeta getFileMeta() {
        return fileMeta;
    }

    public void setFileMeta(FileMeta fileMeta) {
        this.fileMeta = fileMeta;
    }

    public List<String> getNewBlkNames() {
        return newBlkNames;
    }

    public void setNewBlkNames(List<String> newBlkNames) {
        this.newBlkNames = newBlkNames;
    }

    public List<Integer> getNewBlkSizes() {
        return newBlkSizes;
    }

    public void setNewBlkSizes(List<Integer> newBlkSizes) {
        this.newBlkSizes = newBlkSizes;
    }

    public List<List<PhysicalNode>> getNewBlkServers() {
        return newBlkServers;
    }

    public void setNewBlkServers(List<List<PhysicalNode>> newBlkServers) {
        this.newBlkServers = newBlkServers;
    }

}
