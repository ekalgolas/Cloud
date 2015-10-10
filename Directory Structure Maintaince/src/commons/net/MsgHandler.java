package commons.net;

import java.io.IOException;

/**
 * <pre>
 * Created by Yongtao on 8/28/2015.
 *
 * A MsgHandler is called after msg is read out.
 * To maintain integrity of request/response pair, if a handler responses to a request, is should return false
 * in case any consecutive handler responses again thus messes the pair.
 * </pre>
 */
public interface MsgHandler {
	/***
	 * Register by calling IOControl's register methods.
	 *
	 * @param session
	 * @return true if next handler in chain should be called, else false
	 */
	boolean process(Session session) throws IOException;
}
