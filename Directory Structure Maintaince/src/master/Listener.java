package master;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Class to accept a new client`s request and launch a worker thread
 * 	1. Keep on listening to a predefined port to accept client connection
 * 	2. Once a connection is accepted, launch a separate {@link Worker} thread to serve that client
 * 	3. Listen to client
 *
 * @author Ekal.Golas
 */
public class Listener implements Runnable {
	
	public volatile boolean isRunning = true;
	private final ServerSocket listenerSocket;
	private final ArrayList<Thread> workerThreadPool = new ArrayList<Thread>();
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
			try (Socket connectionSocket = listenerSocket.accept()){

				// Launch a worker thread
				Worker worker = new Worker(connectionSocket);
				Thread workerThread = new Thread(worker);
				workerThreadPool.add(workerThread);
				workerThread.start();

			} catch (Exception e) {

			}
		}
		
		for (Thread thread : workerThreadPool) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				e.printStackTrace();
			}
		}

	}
}