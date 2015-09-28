package dht.nio.test;

import java.io.IOException;
import java.util.Random;

import dht.nio.client.TCPClient;
import dht.nio.client.TCPConnection;
import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.ReqType;

public class SimpleClient implements Runnable {
	TCPClient client;

	public SimpleClient() {
		client = new TCPClient();
	}

	public void testRequest() throws IOException {
		TCPConnection con = TCPConnection.getInstance("localhost", 9955);
		ProtocolReq req = new ProtocolReq(ReqType.TABLE);
		int num = 10;
		for (int i = 0; i < num; ++i) {
			req.setrId(i);
			con.request(req);
		}
		ProtocolResp resp = null;
		boolean[] used = new boolean[num];
		int q = 0;
		for (int i = 0; i < num; ++i) {
			resp = (ProtocolResp) con.response();
			System.out.println(resp.toString());
			if (!used[resp.getrId()]) {
				used[resp.getrId()] = true;
				q++;
			}
		}
		if (q != num) {
			System.out.println("req/resp mismatch.");
			System.exit(0);
		}
		try {
			Thread.sleep(new Random().nextInt(500));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		con.close();
		try {
			Thread.sleep(new Random().nextInt(1000));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		while (true) {
			try {
				testRequest();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String args[]) {
		int nThread = 200;
		if (args.length >= 1) {
			nThread = Integer.parseInt(args[0]);
		}
		for (int i = 0; i < nThread; ++i) {
			SimpleClient a = new SimpleClient();
			new Thread(a).start();
		}
	}
}
