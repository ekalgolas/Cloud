package master.dht.dhtfs.server.datanode;

import java.io.IOException;
import java.util.UUID;

import master.dht.dhtfs.core.def.IIDAssigner;

public class TransactionIdAssigner implements IIDAssigner {

    @Override
    public String generateUID() throws IOException {
        return UUID.randomUUID().toString();
    }

}
