package dht.swift.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import dht.dhtfs.client.io.MultiPartBufferInputStreamNew;
import dht.dhtfs.client.io.MultiPartBufferOutputStreamNew;
import dht.dhtfs.client.io.MultiPartFileInputStreamNew;
import dht.dhtfs.client.io.MultiPartFileOutputStreamNew;
import dht.dhtfs.client.io.MultiPartInputStreamNew;
import dht.dhtfs.client.io.MultiPartOutputStreamNew;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.def.IDFSFile;
import dht.dhtfs.core.table.PhysicalNode;
import dht.dhtfs.core.table.RouteTable;
import dht.dhtfs.server.datanode.FileMeta;
import dht.nio.client.TCPConnection;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.RespType;
import dht.nio.protocol.block.ReadFileReq;
import dht.nio.protocol.block.ReadFileResp;
import dht.nio.protocol.block.WriteFileReq;
import dht.nio.protocol.block.WriteFileResp;
import dht.nio.protocol.meta.CommitFileReq;
import dht.nio.protocol.meta.CommitFileResp;
import dht.nio.protocol.meta.CreateFileReq;
import dht.nio.protocol.meta.CreateFileResp;
import dht.nio.protocol.meta.DeleteFileReq;
import dht.nio.protocol.meta.DeleteFileResp;
import dht.nio.protocol.meta.OpenFileReq;
import dht.nio.protocol.meta.OpenFileResp;

public class SwiftFile implements IDFSFile {
	static final int msgBufferSize = 1 << 22;// 4MB
	static final int reqWindowSize = 4;// maximum pending requests

	DhtPath path;
	RouteTable table;
	GeometryLocation location;
	long pointer;
	FileMeta fileMeta;
	HashMap<Integer, TCPConnection> connections;// connections for blocks
	TCPConnection metaServer;
	HashMap<Integer, Long> currentBlkVersion;

	private SwiftFile(DhtPath path, int mode, RouteTable table, GeometryLocation location, TCPConnection metaServer,
			FileMeta fileMeta) {
		this.fileMeta = fileMeta;
		this.path = path;
		// this.mode = mode;
		this.pointer = 0;
		this.table = table;
		this.location = location;
		this.metaServer = metaServer;
		this.connections = new HashMap<Integer, TCPConnection>();
		this.currentBlkVersion = new HashMap<Integer, Long>();
	}

	public FileMeta getFileMeta() {
		return fileMeta;
	}

	@Override
	public void close() throws IOException {
		metaServer.close();
		for (TCPConnection connection : connections.values()) {
			connection.close();
		}

	}

	@Override
	public void setLength(long newLength) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public long getFilePointer() throws IOException {
		return pointer;
	}

	@Override
	public long length() throws IOException {
		return fileMeta.getFileSize();
	}

	@Override
	public int read(byte[] b) throws IOException {
		if (b == null) {
			throw new NullPointerException("b is null");
		}
		return read(b, 0, b.length);
	}

	@Override
	public int read(byte[] b, int offset, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException("b is null");
		}
		if (offset < 0 || len < 0 || len > b.length - offset) {
			throw new IndexOutOfBoundsException("b size: " + b.length + " off: " + offset + " len: " + len);
		}
		if (b.length - offset == 0) {
			return 0;
		}
		MultiPartOutputStreamNew mPartOutputStreamNew = new MultiPartBufferOutputStreamNew(b, offset, len, pointer,
				fileMeta);
		read(mPartOutputStreamNew);
		return (int) mPartOutputStreamNew.getByteWritten();
	}

	@Override
	public void write(byte[] b) throws IOException {
		if (b == null) {
			throw new NullPointerException("b is null");
		}
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int offset, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException("b is null");
		}
		if (offset < 0 || len < 0 || len > b.length - offset) {
			throw new IndexOutOfBoundsException("b size: " + b.length + " offset: " + offset + " len: " + len);
		}
		if (b.length - offset == 0) {
			return;
		}
		MultiPartInputStreamNew is = new MultiPartBufferInputStreamNew(b, offset, len, pointer, fileMeta);
		write(is);

	}

	@Override
	public void seek(long pos) throws IOException {
		if (pos >= fileMeta.getFileSize()) {
			throw new IllegalArgumentException(
					"pos beyond the file size, pos: " + pos + " filesize: " + fileMeta.getFileSize());
		}
		pointer = pos;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit() throws IOException {
		PhysicalNode node = table.getPrimaryNode(path);

		TCPConnection connection = TCPConnection.getInstance(node.getIpAddress(), node.getPort());
		CommitFileReq req = new CommitFileReq(ReqType.COMMIT_FILE);
		req.setFileName(path.getAbsolutePath());
		req.setFileSize(fileMeta.getFileSize());
		req.setBlkNum(fileMeta.getBlkNum());
		req.setBlkVersions(fileMeta.getBlkVersions());
		req.setBlkSizes(fileMeta.getBlkSizes());
		req.setBlkNames(fileMeta.getBlkNames());
		req.setBlkCheckSums(fileMeta.getBlkCheckSums());

		connection.request(req);

		CommitFileResp resp = (CommitFileResp) connection.response();
		if (resp.getResponseType() != RespType.OK) {
			throw new IOException("create file " + path.getPath() + " failed, error: " + resp.getResponseType()
					+ " msg: " + resp.getMsg());
		}
	}

	@Override
	public void download(String dest) throws IOException {
		MultiPartOutputStreamNew os = new MultiPartFileOutputStreamNew(dest, fileMeta.getFileSize(), fileMeta);
		read(os);
	}

	@Override
	public void upload(String src) throws IOException {
		MultiPartInputStreamNew is = new MultiPartFileInputStreamNew(src, fileMeta);
		write(is);
	}

	public static SwiftFile create(DhtPath path, PhysicalNode master, GeometryLocation location) throws IOException {
		// PhysicalNode node = table.getPrimaryNode(path);
		// System.out.println("Node: " + node.getIpAddress() + " "
		// + node.getPort());

		TCPConnection connection = TCPConnection.getInstance(master.getIpAddress(), master.getPort());
		CreateFileReq req = new CreateFileReq(ReqType.CREATE_FILE);
		req.setFileName(path.getAbsolutePath());
		req.setNewBlkNum(1);
		connection.request(req);
		CreateFileResp resp = (CreateFileResp) connection.response();
		if (resp.getResponseType() != RespType.OK) {
			throw new IOException("create file " + path.getPath() + " failed, error: " + resp.getResponseType()
					+ " msg: " + resp.getMsg());
		}

		FileMeta fileMeta = new FileMeta(path.getAbsolutePath());
		// fileMeta.setBlkNames(resp.getNewBlkNames());
		// fileMeta.setBlkNum(resp.getNewBlkNames().size());
		// List<Long> blkVersions = new ArrayList<>();
		// for (int i = 0; i < resp.getNewBlkNames().size(); i++) {
		// blkVersions.add(0l);
		// }
		// fileMeta.setBlkVersions(blkVersions);
		fileMeta.setFileName(path.getAbsolutePath());

		return new SwiftFile(path, READ | WRITE, null, location, connection, fileMeta);
	}

	public static SwiftFile open(DhtPath path, int mode, PhysicalNode master, GeometryLocation location)
			throws IOException {
		// PhysicalNode node = null;
		// if ((mode & IDFSFile.WRITE) != 0) {
		// node = table.getPrimaryNode(path);
		// } else {
		// node = table.getNearestNode(path, location);
		// }
		TCPConnection connection = TCPConnection.getInstance(master.getIpAddress(), master.getPort());
		OpenFileReq req = new OpenFileReq(ReqType.OPEN_FILE);
		req.setFileName(path.getPath());
		connection.request(req);
		OpenFileResp resp = (OpenFileResp) connection.response();
		if (resp.getResponseType() != RespType.OK) {
			throw new IOException("open file " + path.getPath() + " failed, error: " + resp.getResponseType() + " msg: "
					+ resp.getMsg());
		}

		FileMeta fileMeta = new FileMeta(path.getAbsolutePath());
		fileMeta.setBlkNames(resp.getNewBlkNames());
		fileMeta.setBlkNum(resp.getBlkNum());
		// List<Long> blkVersions = new ArrayList<>();
		// for (int i = 0; i < resp.getBlkNames().size(); i++) {
		// blkVersions.add(0l);
		// }
		fileMeta.setBlkVersions(resp.getBlkVersions());
		fileMeta.setFileSize(resp.getFileSize());
		fileMeta.setFileName(resp.getFileName());
		fileMeta.setBlkSizes(resp.getBlkSizes());
		fileMeta.setTime(resp.getFileSize());

		return new SwiftFile(path, mode, null, location, connection, fileMeta);
	}

	public static void delete(DhtPath path, RouteTable table) throws IOException {
		PhysicalNode node = table.getPrimaryNode(path);
		TCPConnection connection = TCPConnection.getInstance(node.getIpAddress(), node.getPort());
		DeleteFileReq req = new DeleteFileReq(ReqType.DELETE_FILE);
		req.setFileName(path.getPath());
		connection.request(req);
		DeleteFileResp resp = (DeleteFileResp) connection.response();
		if (resp.getResponseType() != RespType.OK) {
			throw new IOException("delete file " + path.getPath() + " failed, error: " + resp.getResponseType()
					+ " msg: " + resp.getMsg());
		}
	}

	// DhtPath getBlockPath(int blkId) {
	// return new DhtPath(path.getPath() + "_" + blkId);
	// }

	void read(MultiPartOutputStreamNew os) throws IOException {

		System.out.println("No of segment: " + os.getNoOfSegment());

		ExecutorService executorService = Executors
				.newFixedThreadPool(Math.min(os.getNoOfSegment(), SwiftFileSystem.maxThread));

		Future[] future = new Future[os.getNoOfSegment()];
		for (int i = 0; i < os.getNoOfSegment(); ++i) {
			if (os.bytePending(i) > 0) {
				future[i] = executorService.submit(new MultiPartDownloader(os, i));
			}
		}
		for (int i = 0; i < os.getNoOfSegment(); ++i) {
			try {
				future[i].get();
			} catch (Exception e) {
				System.out.println("Exception: " + e.getMessage());
				e.printStackTrace();

				throw new IOException("download failed, status: " + future[i].isCancelled() + " blockId: " + i);
			}
		}
		executorService.shutdown();
		os.close();
	}

	void write(MultiPartInputStreamNew is) throws IOException {

		ExecutorService executorService = Executors
				.newFixedThreadPool(Math.min(is.getNoOfSegment(), SwiftFileSystem.maxThread));

		Future[] future = new Future[is.getNoOfSegment()];
		for (int i = 0; i < is.getNoOfSegment(); ++i) {
			if (is.remaining(i) > 0) {

				future[i] = executorService.submit(new MultiPartUploader(is, i, fileMeta.getBlkNames().get(i)));
			}
		}
		for (int i = 0; i < is.getNoOfSegment(); ++i) {
			try {
				future[i].get();
			} catch (Exception e) {
				System.out.println("Exception: " + e.getMessage());
				e.printStackTrace();

				throw new IOException("upload failed, status:" + future[i].isCancelled() + " Done:" + future[i].isDone()
						+ " blockId:" + i);
			}
		}
		executorService.shutdown();
		updateFileMeta(is);
		is.close();
	}

	void updateFileMeta(MultiPartInputStreamNew is) {
		pointer += is.getByteRead();
		fileMeta.setFileSize(Math.max(fileMeta.getFileSize(), pointer));

		List<Long> blkSizes = new ArrayList<Long>();
		long blkSize = (pointer / is.getNoOfSegment()) + 1;
		for (int i = 0; i < is.getNoOfSegment(); i++) {

			if (i == (is.getNoOfSegment() - 1)) {
				blkSizes.add(pointer - (i * blkSize));
			} else
				blkSizes.add(blkSize);
		}
		fileMeta.setBlkSizes(blkSizes);
		// fileMeta.setBlkNum((int) ((fileMeta.getFileSize()
		// + fileMeta.getBlkSize() - 1) / fileMeta.getBlkSize()));

		for (Integer blkId : currentBlkVersion.keySet()) {

			System.out.println("currentBlkVersion: " + currentBlkVersion.get(blkId));
			fileMeta.getBlkVersions().add(blkId, currentBlkVersion.get(blkId));
		}
	}

	TCPConnection getConnection(int blkId) throws IOException {
		TCPConnection connection = connections.get(blkId);

		if (connection == null) {
			Vector<PhysicalNode> nodes = table.getAllSortedNodes(path, location);
			OpenFileReq req = new OpenFileReq(ReqType.OPEN_FILE);

			// long versionNumber = getConnectVersion(blkId);
			long versionNumber = fileMeta.getBlkVersions().get(blkId);

			if (versionNumber == -1) {
				throw new IOException("connect version is not specified");
			}

			// DhtPath path = getBlockPath(blkId);

			req.setFileName(path.getPath());

			OpenFileResp resp = null;
			for (PhysicalNode node : nodes) {
				connection = TCPConnection.getInstance(node.getIpAddress(), node.getPort());
				// System.out.println("Node selected: " + node.getIpAddress()
				// + " " + node.getPort());
				connection.request(req);
				resp = (OpenFileResp) connection.response();
				if (resp.getResponseType() == RespType.OK) {
					break;
				}
				connection.close();
			}
			if (resp.getResponseType() != RespType.OK) {
				throw new IOException("no such block version exists, filename: " + req.getFileName() + " blkId: "
						+ blkId + " version: " + versionNumber + " message: " + resp.getMsg());
			}
			connections.put(blkId, connection);
		}
		return connection;
	}

	class MultiPartDownloader extends Thread {
		MultiPartOutputStreamNew os;
		int blkId;

		public MultiPartDownloader(MultiPartOutputStreamNew os, int blkId) {
			this.os = os;
			this.blkId = blkId;
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
			// DhtPath path = getBlockPath(blkId);
			ReadFileReq req = new ReadFileReq(ReqType.READ_FILE);
			ReadFileResp resp;
			req.setBlkName(path.getPath());
			req.setBlkVersion(fileMeta.getBlkVersions().get(blkId));
			int reqId = 0;
			int len;

			TCPConnection connection = getConnection(blkId);

			while ((len = os.moveForward(msgBufferSize, blkId)) != -1 && pendingNum < reqWindowSize) {
				req.setLen(len);
				req.setPos(os.getOffset(blkId) - len);
				req.setrId(reqId++);
				connection.request(req);
				pendingNum++;
			}

			while (pendingNum > 0) {
				resp = (ReadFileResp) connection.response();
				if (resp == null)
					System.out.println("resp null");
				System.out.println(resp);
				os.write(resp.getBuf(), resp.getBuf().length, blkId, 0);
				pendingNum--;
				if ((len = os.moveForward(msgBufferSize, blkId)) != -1) {
					req.setLen(len);
					req.setPos(os.getOffset(blkId) - len);
					req.setrId(reqId++);
					connection.request(req);
					pendingNum++;
				}
			}
		}
	}

	class MultiPartUploader extends Thread {
		MultiPartInputStreamNew is;
		int blkId;
		String blkName;

		public MultiPartUploader(MultiPartInputStreamNew is, int blkId, String blkName) {
			this.is = is;
			this.blkId = blkId;
			this.blkName = blkName;
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
			byte[] buf = new byte[msgBufferSize];
			int pendingNum = 0;
			// DhtPath path = getBlockPath(blkId);
			WriteFileReq req = new WriteFileReq(ReqType.WRITE_FILE);
			WriteFileResp resp;
			req.setBlkName(path.getPath());
			int reqId = 0;
			int len;
			long currentVersionNumber = -1;
			if (currentBlkVersion.get(blkId) != null) {
				currentVersionNumber = currentBlkVersion.get(blkId);
			}

			// long baseVersionNumber = getConnectVersion(blkId);
			long baseVersionNumber = fileMeta.getBlkVersions().get(blkId);

			TCPConnection connection = getConnection(blkId);

			if (currentVersionNumber == -1 && (len = is.read(buf, blkId)) != -1) {
				req.setBuf(buf);
				req.setrId(reqId++);
				req.setBaseBlkVersion(baseVersionNumber);
				// req.setCurrentVersionNumber(-1);
				req.setPos(is.getOffset(blkId) - len);
				req.setLen(len);
				connection.request(req);
				pendingNum++;
				resp = (WriteFileResp) connection.response();
				if (resp.getResponseType() != RespType.OK) {
					throw new IOException("Problem in File Writing at server: " + resp.getMsg());
				}
				pendingNum--;

			}

			while ((len = is.read(buf, blkId)) != -1 && pendingNum < reqWindowSize) {
				req.setBuf(buf);
				req.setrId(reqId++);
				req.setBaseBlkVersion(baseVersionNumber);
				req.setPos(is.getOffset(blkId) - len);
				req.setLen(len);
				connection.request(req);
				pendingNum++;
			}

			while (pendingNum > 0) {
				resp = (WriteFileResp) connection.response();
				pendingNum--;
				if ((len = is.read(buf, blkId)) != -1) {
					req.setBuf(buf);
					req.setrId(reqId++);
					req.setBaseBlkVersion(baseVersionNumber);
					req.setPos(is.getOffset(blkId) - len);
					req.setLen(len);
					connection.request(req);
					pendingNum++;
				}
			}

			baseVersionNumber++;// = resp.getCurrentVersionNumber();
			currentBlkVersion.put(blkId, baseVersionNumber);

		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#insert(byte[])
	 */
	@Override
	public void insert(byte[] b) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#insert(byte[], int, int)
	 */
	@Override
	public void insert(byte[] b, int off, int len) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFile#checkSum()
	 */
	@Override
	public String checkSum() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
