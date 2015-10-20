package master.dht.dhtfs.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.block.ReadFileReq;
import master.dht.nio.protocol.block.ReadFileResp;

public class DataDownloader implements Runnable {
    private ByteBuffer buf;

    private String blkName;
    private long blkVersion;
    private int pos;
    private List<PhysicalNode> nodes;
    private List<Integer> levels;

    public String toString() {
        return "name: " + blkName + " version: " + blkVersion + " pos: " + pos
                + " len: " + (buf.limit() - buf.position());
    }

    public DataDownloader(ByteBuffer buf, String blkName, long blkVersion,
            int pos, List<PhysicalNode> nodes, List<Integer> levels) {
        this.buf = buf;
        this.blkName = blkName;
        this.blkVersion = blkVersion;
        this.pos = pos;
        this.nodes = nodes;
        this.levels = levels;
    }

    @Override
    public void run() {
        try {
            download();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void download() throws IOException {
        int pendingNum = 0;
        ReadFileReq req = new ReadFileReq(ReqType.READ_FILE);
        ReadFileResp resp;
        req.setBlkName(blkName);
        req.setBlkVersion(blkVersion);
        req.setLevel(levels.get(0));
        int reqId = 0, len, offset = pos, remaining = buf.remaining(), beginIdx = buf
                .position();

        TCPConnection connection = null;
        try {
            connection = TCPConnection.getInstance(nodes.get(0).getIpAddress(),
                    nodes.get(0).getPort());
            int windowSize = ClientConfiguration.getReqWindowSize();
            while (remaining > 0 && pendingNum < windowSize) {
                len = Math.min(remaining,
                        ClientConfiguration.getMsgBufferSize());
                req.setLen(len);
                req.setPos(offset);
                req.setrId(reqId++);
                offset += len;
                connection.request(req);
                pendingNum++;
                remaining -= len;
            }

            while (pendingNum > 0) {
                resp = (ReadFileResp) connection.response();
                if (resp.getResponseType() != RespType.OK) {
                    throw new IOException("read blk " + blkName
                            + " failed, error: " + resp.getResponseType()
                            + " msg: " + resp.getMsg());
                }
                buf.position(beginIdx + resp.getPos() - pos);
                buf.put(resp.getBuf());
                pendingNum--;
                if (remaining > 0) {
                    len = Math.min(remaining,
                            ClientConfiguration.getMsgBufferSize());
                    req.setLen(len);
                    req.setPos(offset);
                    req.setrId(reqId++);
                    offset += len;
                    connection.request(req);
                    pendingNum++;
                    remaining -= len;
                }
            }
        } catch (IOException e) {
            throw e;
        } finally {
            connection.close();
        }
    }
}
