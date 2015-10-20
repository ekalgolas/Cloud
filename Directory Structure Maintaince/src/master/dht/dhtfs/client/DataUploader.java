package master.dht.dhtfs.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.block.WriteFileReq;
import master.dht.nio.protocol.block.WriteFileResp;
import master.dht.nio.protocol.block.WriteFinishReq;
import master.dht.nio.protocol.block.WriteFinishResp;
import master.dht.nio.protocol.block.WriteInitReq;
import master.dht.nio.protocol.block.WriteInitResp;

public class DataUploader implements Runnable {
	private ByteBuffer buf;

	private String blkName;
	private long blkVersion;
	private boolean isBaseVersion;
	private int pos;
	private List<PhysicalNode> nodes;
	private List<Integer> levels;

	public String toString() {
		return "name: " + blkName + " version: " + blkVersion + " pos: " + pos
				+ " len: " + (buf.limit() - buf.position());
	}

	public DataUploader(ByteBuffer buf, String blkName, long blkVersion,
			boolean isBaseVersion, int pos, List<PhysicalNode> nodes,
			List<Integer> levels) {
		this.buf = buf;
		this.blkName = blkName;
		this.blkVersion = blkVersion;
		this.isBaseVersion = isBaseVersion;
		this.pos = pos;
		this.nodes = nodes;
		this.levels = levels;
	}

	@Override
	public void run() {
		try {
			upload();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void upload() throws IOException {
		WriteInitReq initReq = new WriteInitReq(ReqType.WRITE_INIT);
		initReq.setBlkName(blkName);
		initReq.setBlkVersion(blkVersion);
		initReq.setBaseVersion(isBaseVersion);
		initReq.setPos(pos);
		initReq.setLen(buf.remaining());
		initReq.setInsert(false);
		initReq.setLevel(levels.get(0));

		List<PhysicalNode> replicas = new ArrayList<PhysicalNode>();
		List<Integer> replicaLevels = new ArrayList<Integer>();
		for (int i = 1; i < nodes.size(); ++i) {
			replicas.add(nodes.get(i));
			replicaLevels.add(levels.get(i));
		}
		initReq.setReplicas(replicas);
		initReq.setReplicaLevels(replicaLevels);

		int pendingNum = 0;
		WriteFileReq req = new WriteFileReq(ReqType.WRITE_FILE);
		WriteFileResp resp;
		int reqId = 0, len, offset = pos, remaining = buf.remaining();

		TCPConnection connection = null;
		try {
			connection = TCPConnection.getInstance(nodes.get(0).getIpAddress(),
					nodes.get(0).getPort());
			connection.request(initReq);

			WriteInitResp initResp = (WriteInitResp) connection.response();
			req.setTransactionId(initResp.getTransactionId());
			byte[] data = new byte[ClientConfiguration.getMsgBufferSize()];
			int windowSize = ClientConfiguration.getReqWindowSize();
			while (remaining > 0 && pendingNum < windowSize) {
				len = Math.min(buf.remaining(),
						ClientConfiguration.getMsgBufferSize());
				req.setPos(offset);
				req.setrId(reqId++);
				if (len < ClientConfiguration.getMsgBufferSize()) {
					data = new byte[len];
				}
				buf.get(data, 0, len);
				req.setBuf(data);
				offset += len;
				connection.request(req);
				pendingNum++;
				remaining -= len;
			}

			while (pendingNum > 0) {
				// System.out.println("waiting for resp");
				resp = (WriteFileResp) connection.response();
				// System.out.println("get for resp");
				if (resp.getResponseType() != RespType.OK) {
					throw new IOException("write blk " + blkName
							+ " failed, error: " + resp.getResponseType()
							+ " msg: " + resp.getMsg());
				}
				pendingNum--;
				if (remaining > 0) {
					len = Math.min(buf.remaining(),
							ClientConfiguration.getMsgBufferSize());
					req.setPos(offset);
					req.setrId(reqId++);
					if (len < ClientConfiguration.getMsgBufferSize()) {
						data = new byte[len];
					}
					buf.get(data, 0, len);
					req.setBuf(data);
					offset += len;
					connection.request(req);
					pendingNum++;
					remaining -= len;
				}
			}
			WriteFinishReq finishReq = new WriteFinishReq(ReqType.WRITE_FINISH);
			finishReq.setTransactionId(req.getTransactionId());
			// System.out.println("sending finishreq: " + blkName + " "
			// + System.currentTimeMillis());
			connection.request(finishReq);
			// System.out.println("sent finishreq: " + blkName + " "
			// + System.currentTimeMillis());
			WriteFinishResp finishResp = (WriteFinishResp) connection
					.response();
			// System.out.println("get finish resp: " + blkName + " "
			// + System.currentTimeMillis());
			List<Integer> updatedLevels = finishResp.getLevels();
			for (int i = 0; i < levels.size(); ++i) {
				levels.set(i, updatedLevels.get(i));
				// System.out.println(i + ": " + updatedLevels.get(i));
			}
		} catch (IOException e) {
			throw e;
		} finally {
			connection.close();
		}
	}
}
