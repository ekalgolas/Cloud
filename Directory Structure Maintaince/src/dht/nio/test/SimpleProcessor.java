package dht.nio.test;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.RespType;
import dht.nio.server.ConnectionInfo;
import dht.nio.server.IProcessor;

public class SimpleProcessor implements IProcessor {

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

    /* (non-Javadoc)
     * @see dht.nio.server.IProcessor#initialize(dht.dhtfs.core.Configuration)
     */
    @Override
    public void initialize(Configuration config) throws IOException {
        // TODO Auto-generated method stub
        
    }

}
