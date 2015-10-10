package commons.sample;

import commons.net.MsgType;

public enum FileWriteMsgType implements MsgType {
	// Client to primary
	WRITE_CHUNK, // id,size,timeout,address
	WRITE_OK, WRITE_FAIL,
	// Primary and secondary
	WRITE_CHUNK_CACHE, // id,size,timeout,address,start,transid,primary
	COMMIT_OK, COMMIT_FAIL // transid
}
