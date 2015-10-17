package commons.sample;

import commons.net.MsgType;

/**
 * Created by Yongtao on 9/20/2015. Extend your msg type like this.
 */
public enum EchoMsgType implements MsgType {
	ECHO, ACK, EXIT_SERVER
}
