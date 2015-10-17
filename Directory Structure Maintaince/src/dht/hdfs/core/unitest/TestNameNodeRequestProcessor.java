package dht.hdfs.core.unitest;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.hdfs.server.namenode.NameNodeRequestProcessor;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.meta.CreateFileReq;
import dht.nio.protocol.meta.CreateFileResp;
import dht.nio.protocol.meta.OpenFileReq;
import dht.nio.protocol.meta.OpenFileResp;
import dht.nio.server.IProcessor;

public class TestNameNodeRequestProcessor {

	public static void main(String[] args) throws IOException {
		String confFile = "conf/masternode.conf";
		Configuration conf = new Configuration();
		conf.initialize(confFile);
		IProcessor processor = new NameNodeRequestProcessor();
		processor.initialize(conf);

		testCreateFile(processor);
	}

	public static void testCreateFile(IProcessor processor) throws IOException {

		CreateFileReq req = new CreateFileReq(ReqType.CREATE_FILE);
		req.setFileName("/a/b/c/d/a.txt");
		req.setNewBlkNum(3);
		CreateFileResp resp = (CreateFileResp) processor.process(null, req);
		resp.dump();

		CreateFileReq req1 = new CreateFileReq(ReqType.CREATE_FILE);
		req1.setFileName("/c/d/e.txt");
		req1.setNewBlkNum(3);
		CreateFileResp resp1 = (CreateFileResp) processor.process(null, req1);
		resp1.dump();

		CreateFileReq req2 = new CreateFileReq(ReqType.CREATE_FILE);
		req2.setFileName("/z/y/x.txt");
		req2.setNewBlkNum(3);
		CreateFileResp resp2 = (CreateFileResp) processor.process(null, req2);
		resp2.dump();

		CreateFileReq req3 = new CreateFileReq(ReqType.CREATE_FILE);
		req3.setFileName("/p/y/x.txt");
		req3.setNewBlkNum(3);
		CreateFileResp resp3 = (CreateFileResp) processor.process(null, req3);
		resp3.dump();

		OpenFileReq openReq = new OpenFileReq(ReqType.OPEN_FILE);
		openReq.setFileName("/a/b/c/d/a.txt");
		OpenFileResp openResp = (OpenFileResp) processor.process(null, openReq);

		OpenFileReq openReq1 = new OpenFileReq(ReqType.OPEN_FILE);
		openReq1.setFileName("/z/y/x.txt");
		OpenFileResp openResp1 = (OpenFileResp) processor.process(null, openReq1);

	}

}
