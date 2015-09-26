package master;

import java.net.Socket;

/**
 * Worker class that serves the client
 * 	1. Given a socket connection, read the client request
 * 	2. Modify the metadata accordingly
 *
 * @author Ekal.Golas
 */
public class Worker implements Runnable {
	
	private final Socket workerSocket;
	
	public Worker(final Socket socket) {
		workerSocket = socket;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		// Read from input stream and then process the commands.
		
	}
}