package commons.net;

import java.io.IOException;

/**
 * <pre>
 * Created by Yongtao on 9/14/2015.
 *
 * A MsgFilter is executed before a msg is read out and is designed to log raw socket info for incoming and replied msg.
 * Extra communication could be done here, but response is handled by handlers.
 * </pre>
 */
public interface MsgFilter {
	/**
	 * This method is invoked in order of filter chain and survives entire life cycle of request and response pair. 
	 * Please remember to call chain.doFilter to pass to next filter. 
	 * Anything executed after calling chain.doFilter will be executed after any MsgHandler and in reversed order of filter chain.
	 *
	 * @param session
	 *            Raw session, only socket and thread pool are usable.
	 * @param chain
	 *            FilterChain to called.
	 * @throws IOException
	 */
	void doFilter(Session session, FilterChain chain) throws IOException;
}
