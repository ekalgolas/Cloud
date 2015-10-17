package commons.net;

import java.io.IOException;

/**
 * Created by Yongtao on 9/14/2015. A MsgFilter should call passed FilterChain
 * object's doFilter method to pass to next filter.
 */
public interface FilterChain {
	void doFilter(Session session, MsgFilter pre) throws IOException;
}
