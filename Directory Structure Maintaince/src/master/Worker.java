package master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

/**
 * Worker class that serves the client
 * 	1. Given a socket connection, read the client request
 * 	2. Modify the metadata accordingly
 *
 * @author Ekal.Golas
 */
public class Worker implements Runnable {
	
	private static final String LS = "ls";
	private static final String MKDIR = "mkdir";
	private static final String RMDIR = "rmdir";
	
	public volatile boolean isRunning = true;
	private final Socket workerSocket;
	private BufferedReader reader;
	private BufferedWriter writer;
	
	public Worker(final Socket socket) {
		workerSocket = socket;

		try {
			reader = new BufferedReader(new InputStreamReader(
					workerSocket.getInputStream()));
			writer = new BufferedWriter(new OutputStreamWriter(
					workerSocket.getOutputStream()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isRunning = false;
		}
		
		
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		// Read from input stream and then process the commands
		while (isRunning) {
			try {
				String command = reader.readLine();
				String reply = "";
				if (command.startsWith(LS)) {
					reply = DirectoryOperations
							.ls(Globals.metadataRoot,command.substring(3));
				}
				else if (command.startsWith(MKDIR)) {
					// Code for mkdir
				}
				else if (command.startsWith(RMDIR)) {
					// Code for rmdir
				}

				writer.write(reply);
				writer.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					reader.close();
					writer.close();
					workerSocket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
}