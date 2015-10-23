package master.dht.nio.server;

import java.io.IOException;

import master.dht.nio.client.TCPClient;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;

public interface IController {

    public ProtocolResp process(ConnectionInfo info, ProtocolReq req);

    public ProtocolReq process(TCPClient client, ProtocolResp resp);

    public void initialize() throws IOException;

}
