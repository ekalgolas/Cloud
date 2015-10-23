package master.dht.dhtfs.core.table;

import master.dht.dhtfs.core.def.IHashFunction;

public class SimpleHash implements IHashFunction {

    private static final long serialVersionUID = 1L;

    private String description;

    public SimpleHash() {
        description = "simple hash";
    }

    @Override
    public int hashValue(String name, int level) {
        return hashValue(name + "_" + level);
    }

    @Override
    public int hashValue(String name) {
        return name.hashCode();
    }

    public String toString() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IHashFunction#getDescription(java.lang.String)
     */
    @Override
    public String getDescription(String description) {
        return description;
    }

}
