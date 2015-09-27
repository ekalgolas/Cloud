package dht.dhtfs.core;

import java.util.UUID;

import dht.dhtfs.core.def.IIDAssigner;

/**
 * @author Yinzi Chen
 * @date May 6, 2014
 */
public class TokenAssigner implements IIDAssigner {

    /*
     * (non-Javadoc)
     * 
     * @see dht.dhtfs.core.def.IIDAssigner#generateUID()
     */
    @Override
    public String generateUID() {
        // TODO Auto-generated method stub
        return UUID.randomUUID().toString();
    }

}
