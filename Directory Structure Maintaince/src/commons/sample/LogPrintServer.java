package commons.sample;

import java.io.File;
import java.io.IOException;

import org.ini4j.Wini;

import commons.net.LogServer;

/**
 * Created by Yongtao on 9/17/2015. Demo log server. It should be started before
 * other clients/servers.
 */
public class LogPrintServer {
	public static void main(final String args[]) {
		try {
			final Wini conf = new Wini(new File("conf/sample/sample.ini"));
			final int port = conf.get("log", "port", int.class);
			new LogServer(port);
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}
}
