package master.dht.nio.protocol.block;

import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class ReadFileResp extends ProtocolResp {

    private static final long serialVersionUID = 1L;

    private int pos;
    private byte[] buf;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("pos: " + pos);
        System.out.println("buf: " + new String(buf));
        System.out.println("***********END***********");
    }

    public ReadFileResp(RespType responseType) {
        super(responseType);
    }

    public ReadFileResp(int rId, RespType responseType) {
        super(rId, responseType);
    }

    public byte[] getBuf() {
        return buf;
    }

    public void setBuf(byte[] buf) {
        this.buf = buf;
    }

    @Override
    public String toString() {
        return "ReadFileResp [buf=" + buf.length + "]";
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

}
