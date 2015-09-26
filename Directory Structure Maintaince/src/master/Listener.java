package master;

/**
 * Class to accept a new client`s request and launch a worker thread
 * 	1. Keep on listening to a predefined port to accept client connection
 * 	2. Once a connection is accepted, launch a separate {@link Worker} thread to serve that client
 * 	3. Listen to client
 *
 * @author Ekal.Golas
 */
public class Listener implements Runnable {
	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
}