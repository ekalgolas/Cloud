package commons.sample.log;

import java.io.File;
import java.io.IOException;
import java.util.logging.Handler;

import org.ini4j.Wini;

import commons.util.Log;
import commons.util.ReconnectSocketHandler;
import commons.util.SingleLineFormatter;

public class Utils{
	public static void connectToLogServer(final Log log) throws IOException{
		final Wini conf=new Wini(new File("conf/sample/sample.ini"));
		final String logIP=conf.get("log","ip");
		final int logPort=conf.get("log","port",int.class);
		// set remote log server to forward all logs there
		final Handler handler=new ReconnectSocketHandler(logIP,logPort);
		handler.setFormatter(new SingleLineFormatter());
		log.getParent().addHandler(handler);
	}
}
