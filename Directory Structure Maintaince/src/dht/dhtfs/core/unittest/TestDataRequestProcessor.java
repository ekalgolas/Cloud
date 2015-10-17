package dht.dhtfs.core.unittest;

import java.util.ArrayList;
import java.util.List;

import dht.dhtfs.server.datanode.DataRequestProcessor;
import dht.nio.protocol.ReqType;
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
import dht.nio.server.IProcessor;

/**
 * @author Yinzi Chen
 * @date May 7, 2014
 */
public class TestDataRequestProcessor {

	public static void main(String[] args) throws Exception {
		// testCreateFile();
		// testOpenFile();
		// testDeleteFile();
		// testCommitFile();
		testReadFile();
		// testWriteFile();
	}

	public static void testCreateFile() {
		IProcessor processor = new DataRequestProcessor();
		CreateFileReq req = new CreateFileReq(ReqType.CREATE_FILE);
		req.setFileName("/a/b/c/a.txt");
		req.setNewBlkNum(3);
		CreateFileResp resp = (CreateFileResp) processor.process(null, req);
		resp.dump();
	}

	public static void testOpenFile() {
		IProcessor processor = new DataRequestProcessor();
		OpenFileReq req = new OpenFileReq(ReqType.OPEN_FILE);
		req.setFileName("/a/b/c/a.txt");
		req.setNewBlkNum(3);
		OpenFileResp resp = (OpenFileResp) processor.process(null, req);
		resp.dump();
	}

	public static void testDeleteFile() {
		IProcessor processor = new DataRequestProcessor();
		DeleteFileReq req = new DeleteFileReq(ReqType.DELETE_FILE);
		req.setFileName("/a/b/c/a.txt");
		DeleteFileResp resp = (DeleteFileResp) processor.process(null, req);
		resp.dump();
	}

	public static void testCommitFile() {
		IProcessor processor = new DataRequestProcessor();
		CommitFileReq req = new CommitFileReq(ReqType.COMMIT_FILE);
		req.setFileName("/a/b/c/a.txt");
		req.setFileSize(166);
		req.setBlkNum(2);

		// ******
		List<Long> blkVersions = new ArrayList<Long>();
		blkVersions.add(3l);
		blkVersions.add(1l);
		req.setBlkVersions(blkVersions);

		// ******
		List<Long> blkSizes = new ArrayList<Long>();
		blkSizes.add(133l);
		blkSizes.add(33l);
		req.setBlkSizes(blkSizes);

		// ******
		List<String> blkNames = new ArrayList<String>();
		blkNames.add("gggggggg-b706-4b32-b45b-909eef3d7f52");
		blkNames.add("35b8074c-e1fb-48c8-b84c-c4a1de93630a");
		req.setBlkNames(blkNames);

		// ******
		List<String> blkCheckSums = new ArrayList<String>();
		blkCheckSums.add("hello");
		blkCheckSums.add("world");
		req.setBlkCheckSums(blkCheckSums);

		CommitFileResp resp = (CommitFileResp) processor.process(null, req);
		resp.dump();
	}

	public static void testReadFile() {
		IProcessor processor = new DataRequestProcessor();
		ReadFileReq req = new ReadFileReq(ReqType.READ_FILE);

		req.setBlkName("/a/b/c/a.txt.0");
		req.setBlkVersion(1l);
		req.setPos(30);
		req.setLen(32);

		ReadFileResp resp = (ReadFileResp) processor.process(null, req);
		resp.dump();
	}

	public static void testWriteFile() {
		IProcessor processor = new DataRequestProcessor();
		WriteFileReq req = new WriteFileReq(ReqType.WRITE_FILE);

		req.setBlkName("/a/b/c/a.txt.0");
		req.setToken("");
		req.setBaseBlkVersion(1l);
		req.setBuf("hello world!".getBytes());
		req.setPos(20);
		req.setInsert(false);

		WriteFileResp resp = (WriteFileResp) processor.process(null, req);
		resp.dump();
	}

}
