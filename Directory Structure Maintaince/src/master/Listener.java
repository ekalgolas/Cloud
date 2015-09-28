package master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * <pre>
 * Class to accept a new client`s request and launch a worker thread
 * 	1. Keep on listening to a predefined port to accept client connection
 * 	2. Once a connection is accepted, launch a separate {@link Worker} thread to serve that client
 * 	3. Listen to client
 * </pre>
 *
 * @author Ekal.Golas
 */
public class Listener implements Runnable {
	public volatile boolean			isRunning			= true;
	private final ServerSocket		listenerSocket;
	private final ArrayList<Thread>	workerThreadPool	= new ArrayList<Thread>();

	/**
	 * Constructor
	 * 
	 * @param socket
	 *            Socket to listen on
	 */
	public Listener(final ServerSocket socket) {
		listenerSocket = socket;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		while (isRunning) {
			try (Socket connectionSocket = listenerSocket.accept()) {
				// Launch a worker thread
				final Worker worker = new Worker(connectionSocket);
				final Thread workerThread = new Thread(worker);
				workerThreadPool.add(workerThread);
				workerThread.start();
			} catch (final Exception e) {
				e.printStackTrace();
			} finally {
				try {
					listenerSocket.close();
				} catch (final IOException e) {
					e.printStackTrace();
				}
			}
		}

		for (final Thread thread : workerThreadPool) {
			try {
				thread.join();
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				e.printStackTrace();
			}
		}
	}
}