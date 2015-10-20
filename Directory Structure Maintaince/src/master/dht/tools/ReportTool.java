package master.dht.tools;

import java.io.IOException;

import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.ServerReportReq;
import master.dht.nio.protocol.ServerReportResp;

public class ReportTool {

    public void testRequest(String ip, int port) throws IOException {
        TCPConnection con = TCPConnection.getInstance(ip, port);
        ServerReportReq req = new ServerReportReq(ReqType.SERVER_REPORT);
        con.request(req);
        ServerReportResp resp = (ServerReportResp) con.response();
        System.out.println(resp.toString());
        con.close();
    }

    public static void main(String args[]) throws IOException {
        String ip = "localhost";
        int port = 7000;
        if (args.length <= 1) {
            System.out.println("Usage: java ReportTool ip port");
            System.exit(-1);
        }
        ip = args[0];
        port = Integer.parseInt(args[1]);
        ReportTool a = new ReportTool();
        a.testRequest(ip, port);
    }

}
