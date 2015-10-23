package master.dht.dhtfs.client;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import master.dht.dhtfs.core.def.IDFSFile;
import master.dht.dhtfs.core.def.IFile;
import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.dhtfs.server.datanode.FileMeta;
import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.meta.BlockLockReq;
import master.dht.nio.protocol.meta.BlockLockResp;
import master.dht.nio.protocol.meta.BlockNameReq;
import master.dht.nio.protocol.meta.BlockNameResp;
import master.dht.nio.protocol.meta.CommitFileReq;
import master.dht.nio.protocol.meta.CommitFileResp;

public class DHTFile implements IDFSFile {

	private PhysicalNode metaServer;
	private FileMeta fileMeta;

	private long pointer;
	private int mode;
	private int preferredBlkSize;

	private Set<String> blkLocks;

	private Set<Integer> updated;

	protected DHTFile(int mode, PhysicalNode metaServer, FileMeta fileMeta,
			List<Integer> newBlkSizes, List<String> newBlkNames,
			List<List<PhysicalNode>> newBlkServers) {
		this.mode = mode;
		this.metaServer = metaServer;
		this.fileMeta = fileMeta;
		this.pointer = 0;
		this.preferredBlkSize = ClientConfiguration.getPreferredBlkSize();
		this.updated = new HashSet<Integer>();
		this.blkLocks = new HashSet<String>();
	}

	public String toString() {
		return fileMeta.toString();
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public int read(byte[] b, int offset, int len) throws IOException {
		len = (int) Math.min(len, fileMeta.getFileSize() - pointer);
		if (len == 0) {
			return -1;
		}
		ByteBuffer buf = ByteBuffer.wrap(b, offset, len);
		MultipartDownloader downloader = new MultipartDownloader();
		downloader.register(buf, fileMeta, pointer, updated);
		pointer += buf.limit() - buf.position();
		downloader.download();
		return len;
	}

	@Override
	public void download(String dest) throws IOException {
		pointer = 0;
		RandomAccessFile memoryMappedFile = new RandomAccessFile(dest, "rw");
		long fileSize = fileMeta.getFileSize();
		// System.out.println("fileSize: " + fileSize);
		long offset = 0;
		MultipartDownloader downloader = new MultipartDownloader();
		while (offset < fileSize) {
			ByteBuffer buf = memoryMappedFile.getChannel().map(
					FileChannel.MapMode.READ_WRITE, offset,
					Math.min(offset + Integer.MAX_VALUE, fileSize) - offset);
			downloader.register(buf, fileMeta, pointer, updated);
			pointer += buf.limit() - buf.position();
			offset += Integer.MAX_VALUE;
		}
		downloader.download();
		memoryMappedFile.close();
	}

	@Override
	public void lock(long pos, long len) throws IOException {
		TCPConnection connection = TCPConnection.getInstance(
				metaServer.getIpAddress(), metaServer.getPort());
		BlockLockReq req = new BlockLockReq(ReqType.BLOCK_LOCK);
		List<String> locks = getLockList(pos, len);
		List<String> lockNeed = new ArrayList<String>();
		for (int i = 0; i < locks.size(); ++i) {
			if (!blkLocks.contains(locks.get(i))) {
				lockNeed.add(locks.get(i));
			}
		}
		req.setBlkNames(lockNeed);
		req.setFileName(fileMeta.getFileName());
		connection.request(req);
		BlockLockResp resp = (BlockLockResp) connection.response();
		if (resp.getResponseType() != RespType.OK) {
			throw new IOException("lock file " + fileMeta.getFileName()
					+ " failed, error: " + resp.getResponseType() + " msg: "
					+ resp.getMsg());
		}
		for (String name : lockNeed) {
			blkLocks.add(name);
		}
	}

	private List<String> getLockList(long pos, long len) {
		List<String> locks = new ArrayList<String>();
		List<Integer> blkSizes = fileMeta.getBlkSizes();
		List<String> blkNames = fileMeta.getBlkNames();
		int blkNum = blkSizes.size();
		long sum = 0;
		for (int i = 0; i < blkNum; ++i, sum += blkSizes.get(i)) {
			if (pos + len <= sum || sum + blkSizes.get(i) <= pos) {
				continue;
			}
			locks.add(blkNames.get(i));
		}
		return locks;
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	private boolean checkLock(long pos, long len, boolean isInsert) {
		if (!isInsert) {
			List<String> locks = getLockList(pos, len);
			for (String name : locks) {
				if (!blkLocks.contains(name)) {
					return false;
				}
			}
			return true;
		} else {
			List<Integer> blkSizes = fileMeta.getBlkSizes();
			List<String> blkNames = fileMeta.getBlkNames();
			int blkNum = blkSizes.size();
			long sum = 0;
			for (int i = 0; i < blkNum; ++i, sum += blkSizes.get(i)) {
				if (sum <= pos && pos < sum + blkSizes.get(i)) {
					return blkLocks.contains(blkNames.get(i));
				}
			}
			return false;
		}
	}

	@Override
	public void write(byte[] b, int offset, int len) throws IOException {
		if (!checkLock(offset, len, false)) {
			throw new IOException("need to get lock before writing");
		}
		assignNewBlks(len);
		ByteBuffer buf = ByteBuffer.wrap(b, offset, len);
		MultipartUploader uploader = new MultipartUploader();
		uploader.register(buf, fileMeta, pointer, updated);
		pointer += buf.limit() - buf.position();
		uploader.upload();
	}

	@Override
	public void upload(String src) throws IOException {
		if ((CREATE & mode) == 0) {
			throw new IOException("file already exists: "
					+ fileMeta.getFileName());
		}
		pointer = 0;
		RandomAccessFile memoryMappedFile = new RandomAccessFile(src, "r");
		long fileSize = memoryMappedFile.length();
		assignNewBlks(fileSize);
		long offset = 0;
		MultipartUploader uploader = new MultipartUploader();
		while (offset < fileSize) {
			ByteBuffer buf = memoryMappedFile.getChannel().map(
					FileChannel.MapMode.READ_ONLY, offset,
					Math.min(offset + Integer.MAX_VALUE, fileSize) - offset);
			uploader.register(buf, fileMeta, pointer, updated);
			pointer += buf.limit() - buf.position();
			offset += Integer.MAX_VALUE;
		}
		uploader.upload();
		memoryMappedFile.close();
	}

	@Override
	public void commit() throws IOException {
		TCPConnection connection = TCPConnection.getInstance(
				metaServer.getIpAddress(), metaServer.getPort());
		CommitFileReq req = new CommitFileReq(ReqType.COMMIT_FILE);
		List<String> locks = new ArrayList<String>();
		for (String name : blkLocks) {
			locks.add(name);
		}
		req.setFileMeta(fileMeta);
		req.setLocks(locks);
		for (int i = 0; i < fileMeta.getBlkNum(); ++i) {
			if (updated.contains(i))
				fileMeta.getBlkVersions().set(i,
						fileMeta.getBlkVersions().get(i) + 1);
		}

		connection.request(req);

		CommitFileResp resp = (CommitFileResp) connection.response();
		if (resp.getResponseType() != RespType.OK) {
			throw new IOException("commit file " + fileMeta.getFileName()
					+ " failed, error: " + resp.getResponseType() + " msg: "
					+ resp.getMsg());
		}
		fileMeta = resp.getFileMeta();
		pointer = 0;
		mode = IFile.READ;
		updated.clear();
		blkLocks.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see master.dht.dhtfs.core.def.IFile#insert(byte[])
	 */
	@Override
	public void insert(byte[] b) throws IOException {
		insert(b, 0, b.length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see master.dht.dhtfs.core.def.IFile#insert(byte[], int, int)
	 */
	@Override
	public void insert(byte[] b, int off, int len) throws IOException {
		if (!checkLock(off, len, true)) {
			throw new IOException("need to get lock before writing");
		}
		if (pointer == fileMeta.getFileSize()) {
			write(b, off, len);
			return;
		}
		int blkIdx = 0;
		int pos = 0;
		if (pointer > 0) {
			long sum = 0;
			while (sum <= pointer) {
				sum += fileMeta.getBlkSizes().get(blkIdx++);
			}
			sum -= fileMeta.getBlkSizes().get(--blkIdx);
			pos = (int) (pointer - sum);
		}
		ByteBuffer buf = ByteBuffer.wrap(b, off, len);

		long fileSize = fileMeta.getFileSize() + len;
		int blkSize = fileMeta.getBlkSizes().get(blkIdx) + len;
		fileMeta.setFileSize(fileSize);
		fileMeta.getBlkSizes().set(blkIdx, blkSize);
		pointer += len;

		long blkVersion = updated.contains(blkIdx) ? fileMeta.getBlkVersions()
				.get(blkIdx) + 1 : fileMeta.getBlkVersions().get(blkIdx);
		DataInsertUploader uploader = new DataInsertUploader(buf, fileMeta
				.getBlkNames().get(blkIdx), blkVersion,
				!updated.contains(blkIdx), pos, fileMeta.getBlkServers().get(
						blkIdx), fileMeta.getBlkLevels().get(blkIdx));
		updated.add(blkIdx);
		uploader.upload();
	}

	@Override
	public long getFilePointer() throws IOException {
		return pointer;
	}

	@Override
	public void seek(long pos) throws IOException {
		if (pos > fileMeta.getFileSize()) {
			throw new IllegalArgumentException(
					"pos beyond the file size, pos: " + pos + " filesize: "
							+ fileMeta.getFileSize());
		}
		pointer = pos;
	}

	@Override
	public long length() throws IOException {
		return fileMeta.getFileSize();
	}

	@Override
	public void setLength(long newLength) throws IOException {
		throw new IOException("do not support");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see master.dht.dhtfs.core.def.IFile#checkSum()
	 */
	@Override
	public String checkSum() throws IOException {
		throw new IOException("do not support");
	}

	@Override
	public void flush() throws IOException {
		throw new IOException("do not support");
	}

	public void setPreferredBlkSize(int preferredBlkSize) {
		this.preferredBlkSize = preferredBlkSize;
	}

	private void assignNewBlks(long bytesToWrite) throws IOException {
		long bytesToAdd = bytesToWrite + pointer - fileMeta.getFileSize();
		if (bytesToAdd <= 0) {
			return;
		}
		List<Integer> blkSizes = fileMeta.getBlkSizes();
		long fileSize = fileMeta.getFileSize();
		fileMeta.setFileSize(fileSize + bytesToAdd);
		if (fileSize > 0) {
			int lastBlkSize = blkSizes.get(blkSizes.size() - 1);
			if (lastBlkSize + bytesToAdd <= preferredBlkSize) {
				blkSizes.set(blkSizes.size() - 1, lastBlkSize
						+ (int) bytesToAdd);
				return;
			}
			if (lastBlkSize < preferredBlkSize) {
				blkSizes.set(blkSizes.size() - 1, preferredBlkSize);
				bytesToAdd -= preferredBlkSize - lastBlkSize;
			}
		}
		TCPConnection connection = TCPConnection.getInstance(
				metaServer.getIpAddress(), metaServer.getPort());
		BlockNameReq req = new BlockNameReq(ReqType.NEW_BLOCK);
		req.setPreferredBlkSize(preferredBlkSize);
		req.setBytesToAdd(bytesToAdd);
		connection.request(req);
		BlockNameResp resp = (BlockNameResp) connection.response();
		// resp.dump();
		updateFileMeta(resp.getNewBlkSizes().size(), resp.getNewBlkSizes(),
				resp.getNewBlkNames(), resp.getNewBlkServers());
		if (resp.getResponseType() != RespType.OK) {
			throw new IOException("block name req for file "
					+ fileMeta.getFileName() + " failed, error: "
					+ resp.getResponseType() + " msg: " + resp.getMsg());
		}
	}

	private void updateFileMeta(int num, List<Integer> newBlkSizes,
			List<String> newBlkNames, List<List<PhysicalNode>> newBlkServers) {
		fileMeta.getBlkSizes().addAll(newBlkSizes);
		fileMeta.getBlkNames().addAll(newBlkNames);
		fileMeta.getBlkServers().addAll(newBlkServers);
		fileMeta.setBlkNum(fileMeta.getBlkNum() + num);
		for (int i = 0; i < num; ++i) {
			fileMeta.getBlkVersions().add(0l);
			fileMeta.getBlkCheckSums().add(null);
			List<Integer> blkLevels = new ArrayList<Integer>();
			for (int j = 0; j < newBlkServers.get(i).size(); ++j) {
				blkLevels.add(-1);
			}
			fileMeta.getBlkLevels().add(blkLevels);

		}
	}
}
