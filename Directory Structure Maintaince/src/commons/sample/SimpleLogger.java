package commons.sample;

import java.net.Socket;

import commons.net.MsgHandler;
import commons.net.MsgType;
import commons.net.Session;
import commons.util.Log;

/**
 * Created by Yongtao on 9/10/2015.
 * <p/>
 * This is sample msg handler simply recording incoming session info.
 */
public class SimpleLogger implements MsgHandler {
	private static Log	log	= Log.get();

	@Override
	public boolean process(final Session session) {
		final Socket socket = session.getSocket();
		final String ip = socket.getInetAddress()
				.getHostAddress();
		final int port = socket.getPort();
		final MsgType type = session.getType();
		log.i("Receive " + type + " from " + ip + ":" + port + " " + session.getKeyValuePairs());
		return true;
	}
}
