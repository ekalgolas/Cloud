package dht.nio.server;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;

public interface IProcessor {

    public ProtocolResp process(ConnectionInfo info, ProtocolReq req);

    public void initialize(Configuration config) throws IOException;

}
