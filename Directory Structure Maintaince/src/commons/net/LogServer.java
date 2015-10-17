package commons.net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import commons.util.Log;

/**
 * Created by Yongtao on 9/17/2015. A simple log server to print received log to
 * console, if you need to do more, modify code in Worker.
 */
public class LogServer {
	private static final ExecutorService pool = Executors.newCachedThreadPool();
	private static final Log log = Log.get();

	public LogServer(final int port) throws IOException {
		final ServerSocketChannel server = ServerSocketChannel.open();
		final Selector selector = Selector.open();
		server.socket().bind(new InetSocketAddress(port));
		server.configureBlocking(false);
		server.register(selector, SelectionKey.OP_ACCEPT);
		final Coordinator coordinator = new Coordinator(selector);
		pool.execute(coordinator);
	}

	class Coordinator implements Runnable {
		private final Selector selector;

		Coordinator(final Selector selector) {
			this.selector = selector;
		}

		@Override
		public void run() {
			for (;;) {
				int count;
				try {
					count = selector.select();
				} catch (final IOException e) {
					log.w(e);
					break;
				}
				if (count != 0) {
					final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
					while (it.hasNext()) {
						final SelectionKey key = it.next();
						it.remove();
						final ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
						SocketChannel channel;
						try {
							channel = ssc.accept();
						} catch (final IOException ignored) {
							continue;
						}
						final Worker worker = new Worker(channel);
						pool.execute(worker);
					}
				}
			}
		}
	}

	class Worker implements Runnable {
		private final SocketChannel socketChannel;

		Worker(final SocketChannel socketChannel) {
			this.socketChannel = socketChannel;
		}

		@Override
		public void run() {
			final Socket socket = socketChannel.socket();
			try {
				final InputStream is = socket.getInputStream();
				final BufferedReader br = new BufferedReader(new InputStreamReader(is));
				String line;
				final String ip = socket.getInetAddress().getHostAddress();
				final int port = socket.getPort();
				final String prefix = "[" + ip + ":" + port + "]";
				while ((line = br.readLine()) != null) {
					System.out.println(prefix + line);
					// todo save to log file, etc
				}
			} catch (final IOException ignored) {
			} finally {
				try {
					socket.close();
				} catch (final IOException ignored) {
				}
			}
		}
	}
}
