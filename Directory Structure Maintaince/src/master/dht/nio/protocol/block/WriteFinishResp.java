package master.dht.nio.protocol.block;

import java.util.List;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class WriteFinishResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;
    private List<Integer> levels;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("***********END***********");
    }

    public WriteFinishResp(RespType responseType) {
        super(responseType);
    }

    public WriteFinishResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public List<Integer> getLevels() {
        return levels;
    }

    public void setLevels(List<Integer> levels) {
        this.levels = levels;
    }

}
