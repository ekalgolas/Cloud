package commons.sample;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.ini4j.Wini;

import commons.net.IOControl;
import commons.net.Session;
import commons.sample.log.Utils;
import commons.util.Log;

/**
 * Created by Yongtao on 9/10/2015.
 * <p/>
 * This is demo how to use IOControl as client.
 */
public class EchoClient {
	private static Log log = Log.get();

	public static void main(final String args[]) {
		try {
			Utils.connectToLogServer(log);
			// read conf file here
			final Wini conf = new Wini(new File("conf/sample/sample.ini"));
			final String serverIP = conf.get("read server", "ip");
			final int serverPort = conf.get("read server", "port", int.class);

			try {
				final IOControl control = new IOControl();
				// get what you type
				final Scanner in = new Scanner(System.in);
				for (;;) {
					final String cmd = in.nextLine();
					if (cmd.length() > 0) {
						final String test = cmd.toLowerCase().trim();
						// ask server to quit
						if (test.equals("quit") || test.equals("exit") || test.equals("q") || test.equals("e")) {
							control.send(new Session(EchoMsgType.EXIT_SERVER), serverIP, serverPort);
							break;
						}
					}
					// else just send plain ping msg.
					final Session session = new Session(EchoMsgType.ECHO);
					session.set("Comment", cmd);
					final Session ping = control.request(session, serverIP, serverPort);
					log.i("Heard: " + ping.getType());
				}
			} catch (final Exception e) {
				log.w(e);
			}
		} catch (final IOException e) {
			log.w(e);
		}
	}
}
