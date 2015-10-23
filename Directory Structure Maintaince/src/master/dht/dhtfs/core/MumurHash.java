package master.dht.dhtfs.core;

import master.dht.dhtfs.core.def.IHashFunction;

/**
 * @author Yinzi Chen
 * @date May 7, 2014
 */
public class MumurHash implements IHashFunction {

    private static final long serialVersionUID = 1L;

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IHashFunction#hashValue(java.lang.String)
     */
    @Override
    public int hashValue(String name) {
        return name.hashCode();
    }

    @Override
    public int hashValue(String name, int level) {
        return hashValue(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IHashFunction#setDescription(java.lang.String)
     */
    @Override
    public void setDescription(String description) {

    }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.IHashFunction#getDescription(java.lang.String)
     */
    @Override
    public String getDescription(String description) {
        return "MumurHash 3a    2.7 GB/s     10       Austin Appleby";
    }

}
