package master.dht.dhtfs.server.datanode;

import master.dht.dhtfs.core.Saveable;

public class IncrementalLong extends Saveable {

    private static final long serialVersionUID = 1L;

    private long currentId;

    public IncrementalLong(long id) {
        currentId = id;
    }

    public long getCurrentId() {
        return currentId;
    }

    public void setCurrentId(long currentId) {
        this.currentId = currentId;
    }

    public void increase(int val) {
        currentId += val;
    }

}
