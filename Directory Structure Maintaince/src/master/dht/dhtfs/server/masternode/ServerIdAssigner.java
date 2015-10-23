package master.dht.dhtfs.server.masternode;

import java.io.File;
import java.io.IOException;

import master.dht.dhtfs.core.def.IIDAssigner;
import master.dht.dhtfs.server.datanode.IncrementalLong;

public class ServerIdAssigner implements IIDAssigner {
    private static final int firstId = 10000;
    private String file;

    public ServerIdAssigner(String idFile) throws IOException {
        this.file = idFile;
        if (!new File(file).exists()) {
            IncrementalLong serverId = new IncrementalLong(firstId);
            serverId.save(file);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.INameAssigner#generateUID()
     */
    @Override
    public synchronized String generateUID() throws IOException {
        IncrementalLong serverId = (IncrementalLong) IncrementalLong
                .loadMeta(file);
        String uid = Long.toString(serverId.getCurrentId());
        serverId.increase(1);
        serverId.save(file);
        return uid;
    }

}
