package master;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * <pre>
 * Class to implement the master server
 * Following is the functionality of the master :-
 * 	1. Read the existing deserialized data file of the existing directory structure
 * 	2. Serialize and create a global metadata object
 * 	3. Launch a {@link Listener} thread
 * 	4. Serve the client
 * </pre>
 */
public class Master {
	private static final String	INPUT_DIR_STRUCT	= "./data/out.txt";
	private static final int	LISTENER_PORT		= 18000;

	private static ServerSocket	listenerSocket		= null;

	public static void initializeMaster() {

		if (listenerSocket != null) {
			return;
		}

		try {
			listenerSocket = new ServerSocket(LISTENER_PORT);
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Master`s main method
	 *
	 * @param args
	 *            Command line arguments
	 */
	public static void main(final String[] args) {
		// Generate metadata for existing directory structure
		DirectoryParser.parseText(INPUT_DIR_STRUCT);

		// TODO : Set the generated metadata to the global variable

		// Launch listener to process input requests
		final Listener listener = new Listener(listenerSocket);
		final Thread listenerThread = new Thread(listener);
		listenerThread.start();

		try {
			listenerThread.join();
		} catch (final InterruptedException e) {
			Thread.currentThread()
				.interrupt();
			e.printStackTrace();
		}

	}
}