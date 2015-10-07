package commons.sample;

import commons.net.MsgType;

/**
 * Created by Yongtao on 9/20/2015.
 * <p/>
 * File read msg
 */
public enum FileReadMsgType implements MsgType {
	READ_FILE, // path, [position], [limit]
	READ_FILE_OK, READ_FILE_ERROR
}
