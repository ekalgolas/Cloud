package commons.sample;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import org.ini4j.Wini;

import commons.net.IOControl;
import commons.net.MsgFilter;
import commons.net.MsgHandler;
import commons.net.Session;
import commons.sample.log.Utils;
import commons.util.FileHelper;
import commons.util.Log;

/**
 * Created by Yongtao on 9/10/2015.
 * <p/>
 * This is demo for using IOControl as server.
 */
public class FileReadEchoServer {
	private static Log log = Log.get();

	/**
	 * Deal with echo and exit msg
	 */
	static class Echo implements MsgHandler {
		private final IOControl control;

		Echo(final IOControl control) {
			this.control = control;
		}

		@Override
		public boolean process(final Session session) throws IOException {
			control.response(new Session(EchoMsgType.ACK), session);
			if (session.getType() == EchoMsgType.EXIT_SERVER) {
				control.quitServer();
			}
			return false;
		}
	}

	/**
	 * Handle file read.
	 */
	static class FileServer implements MsgHandler {
		private final IOControl control;

		FileServer(final IOControl control) {
			this.control = control;
		}

		@Override
		public boolean process(final Session session) throws IOException {
			final String path = session.getString("path");
			File file;
			final Session error = new Session(FileReadMsgType.READ_FILE_ERROR);
			try {
				file = new File(path);
				final FileInputStream fis = new FileInputStream(file);
				final Session reply = new Session(FileReadMsgType.READ_FILE_OK);
				final long fileSize = file.length();
				final long position = session.getLong("position", 0);
				long limit = session.getLong("limit", fileSize);
				if (limit > fileSize) {
					limit = fileSize;
				}
				reply.set("name", file.getName());
				reply.set("size", limit);
				reply.set("modify", file.lastModified());
				control.response(reply, session);
				final SocketChannel channel = session.getSocketChannel();
				final FileChannel fc = fis.getChannel();
				// here is DMA copying utilizing sendfile system call.
				FileHelper.upload(fc, channel, limit, position);
			} catch (final Exception e) {
				log.w(e);
				error.set("comment", e.getMessage());
				control.response(error, session);
			}
			return false;
		}
	}

	public static void main(final String args[]) {
		try {
			Utils.connectToLogServer(log);
			// read conf
			final Wini conf = new Wini(new File("conf/sample/sample.ini"));
			final int port = conf.get("read server", "port", int.class);

			try {
				final IOControl server = new IOControl();
				// register echo handlers
				final MsgHandler logger = new Echo(server);
				server.registerMsgHandlerLast(logger, new EchoMsgType[] { EchoMsgType.ECHO, EchoMsgType.EXIT_SERVER });

				// register filters
				final MsgFilter stat = new RawLogger();
				server.registerMsgFilterHead(stat);

				// register file read handler
				final MsgHandler fileRead = new FileServer(server);
				server.registerMsgHandlerHead(fileRead, FileReadMsgType.READ_FILE);

				// start server
				server.startServer(port);

				// blocking until asked to quit (see SimpleEchoClient)
				server.waitForServer();
			} catch (final IOException e) {
				log.w(e);
			}
		} catch (final IOException e) {
			log.w(e);
		}
	}
}
