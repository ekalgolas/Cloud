package master.dht.nio.test;

import java.io.IOException;

import master.dht.nio.client.TCPClient;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;
import master.dht.nio.server.ConnectionInfo;
import master.dht.nio.server.IController;

public class SimpleProcessor implements IController {

    @Override
    public ProtocolResp process(ConnectionInfo info, ProtocolReq req) {
        System.out.println("zouni!");
        // try {
        // int t = new Random().nextInt(3) * 1000;
        // System.out.println("sleep: " + t / 1000.0);
        // Thread.sleep(t);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        System.out.println(req.toString());
        ProtocolResp resp = new ProtocolResp(req.getrId(), RespType.OK);
        return resp;
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.nio.server.IProcessor#initialize(master.dht.dhtfs.core.Configuration)
     */
    @Override
    public void initialize() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public ProtocolReq process(TCPClient client, ProtocolResp resp) {
        // TODO Auto-generated method stub
        return null;
    }

}
