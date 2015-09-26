package master;

/**
 * Class to implement the master server
 * Following is the functionality of the master :-
 * 	1. Read the existing deserialized data file of the existing directory structure
 * 	2. Serialize and create a global metadata object
 * 	3. Launch a {@link Listener} thread
 * 	4. Serve the client
 *
 * @author Ekal.Golas
 *
 */
public class Master {
	public static final String INPUT_DIR_STRUCT = "./data/out.txt";
	
	/**
	 * Master`s main method
	 *
	 * @param args
	 *            	Command line arguments
	 */
	public static void main(final String[] args) {
		// TODO Auto-generated method stub
		
		DirectoryParser.parseText(INPUT_DIR_STRUCT);

	}
}