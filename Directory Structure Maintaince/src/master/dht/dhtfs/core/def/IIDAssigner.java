package master.dht.dhtfs.core.def;

import java.io.IOException;

/**
 * @author Yinzi Chen
 * @date May 6, 2014
 */
public interface IIDAssigner {

    public String generateUID() throws IOException;

}
