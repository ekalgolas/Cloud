package commons.sample;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

import commons.net.FilterChain;
import commons.net.MsgFilter;
import commons.net.Session;
import commons.sample.log.SocketProxy;
import commons.util.Log;

/**
 * Created by Yongtao on 9/13/2015.
 * <p/>
 * This is sample for simple msg filter. You can modify the code to record load/performance data.
 */
public class RawLogger implements MsgFilter {
	private static Log	log			= Log.get();
	static AtomicLong	inCounter	= new AtomicLong(0);
	static AtomicLong	outCounter	= new AtomicLong(0);

	@Override
	public void doFilter(final Session session, final FilterChain chain) throws IOException {
		try {
			if (!(session.getSocket() instanceof SocketProxy)) {
				final Socket proxy = new SocketProxy(session.getSocket(), inCounter, outCounter);
				session.setSocket(proxy);
			}
		} catch (final IOException e) {
			log.w(e);
		}
		chain.doFilter(session, this);
		log.i("So far total read: " + inCounter + " bytes, upload: " + outCounter + " bytes");
	}
}
