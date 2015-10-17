package dht.dhtfs.core;

import java.util.UUID;

import dht.dhtfs.core.def.IIDAssigner;

/**
 * @author Yinzi Chen
 * @date May 6, 2014
 */
public class BlockNameAssigner implements IIDAssigner {

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.INameAssigner#generateUID()
	 */
	@Override
	public String generateUID() {
		// TODO
		return UUID.randomUUID().toString();
	}
}
