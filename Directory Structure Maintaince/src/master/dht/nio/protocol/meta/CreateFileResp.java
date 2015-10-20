package master.dht.nio.protocol.meta;

import java.util.List;

import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class CreateFileResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    private List<Integer> levels;
    private List<Integer> newBlkSizes;
    private List<String> newBlkNames;
    private List<List<PhysicalNode>> newBlkServers;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("***********END***********");
    }

    public CreateFileResp(RespType responseType) {
        super(responseType);
    }

    public CreateFileResp(int rId, RespType responseType) {
        super(rId, responseType);
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

    public List<Integer> getLevels() {
        return levels;
    }

    public void setLevels(List<Integer> levels) {
        this.levels = levels;
    }

}
