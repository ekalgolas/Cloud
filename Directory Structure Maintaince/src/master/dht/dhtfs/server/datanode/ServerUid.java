package master.dht.dhtfs.server.datanode;

import master.dht.dhtfs.core.Saveable;

public class ServerUid extends Saveable {

    private static final long serialVersionUID = 1L;

    private String serverId;

    public ServerUid(String serverId) {
        this.serverId = serverId;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

}
