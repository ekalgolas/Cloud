package dht.tools;

import java.io.IOException;

import dht.nio.client.TCPClient;
import dht.nio.client.TCPConnection;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.ServerReportReq;
import dht.nio.protocol.ServerReportResp;

public class ReportTool {
	TCPClient client;

	public ReportTool() {
		client = new TCPClient();
	}

	public void testRequest(int port) throws IOException {
		TCPConnection con = TCPConnection.getInstance("localhost", port);
		ServerReportReq req = new ServerReportReq(ReqType.SERVER_REPORT);
		con.request(req);
		ServerReportResp resp = (ServerReportResp) con.response();
		System.out.println(resp.toString());
		con.close();
	}

	public static void main(String args[]) throws IOException {
		int port = 7000;
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		}
		ReportTool a = new ReportTool();
		a.testRequest(port);
	}

}
