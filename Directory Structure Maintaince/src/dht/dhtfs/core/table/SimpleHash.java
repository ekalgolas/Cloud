package dht.dhtfs.core.table;

import dht.dhtfs.core.def.IHashFunction;

public class SimpleHash implements IHashFunction {

    private static final long serialVersionUID = 1L;

    private String description;

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
     * @see dht.dhtfs.core.def.IHashFunction#getDescription(java.lang.String)
     */
    @Override
    public String getDescription(String description) {
        // TODO Auto-generated method stub
        return null;
    }

}
