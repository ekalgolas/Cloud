package master.nfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import org.apache.log4j.Logger;


import com.sun.media.sound.InvalidDataException;

import commons.AppConfig;
import commons.Message;
import commons.OutputFormatter;
import commons.dir.Directory;
import commons.dir.ICommandOperations;

public class NFSDirectoryOperations implements ICommandOperations {
	private final static Logger		LOGGER		= Logger.getLogger(NFSDirectoryOperations.class);
	private static final int		CUT_LEVEL	= Integer.parseInt(AppConfig.getValue("server.dht.cutLevel"));
	private static final String NFS_FOLDER = "/Users/sahith/Desktop/cloudsharedc3";

	@Override
	public Message ls(final Directory root,
			 String filePath,
			final String... arguments)
					throws InvalidPropertiesFormatException,
					InvalidDataException {
	
		filePath = filePath.replace(" ", "-");

		String line;
		final StringBuilder pathname = new StringBuilder();
		final String[] names1 = filePath.split("/");
		final List<String> names = new ArrayList<String>();
			names.addAll(Arrays.asList(names1));
		final String path = getPath(filePath, pathname, names, names.size() - 1);
		
//		Path filepathname = Paths.get(Paths.get(NFS_FOLDER, path + ".txt").toString());
//		FileChannel fileChannel;
//		try {
//			 fileChannel = FileChannel.open(filepathname, StandardOpenOption.READ);
//		} catch (IOException e1) {
//			System.out.println("failed obtaining filechannel");
//		}
//		System.out.println("File channel opened for read. Acquiring lock...");
//		
//		FileLock lock = fileChannel.lock(0,Long.MAX_VALUE, true);
//		
//		System.out.println("Lock acquired: " + lock.isValid());
//		System.out.println("Lock is shared: " + lock.isShared());

		
		final File file = new File(Paths.get(NFS_FOLDER, path + ".txt").toString());
	
		final Message builder = new Message("Listing for " + filePath + "\n");
		final OutputFormatter output = new OutputFormatter();
		output.addRow("TYPE", "NAME");

		String filenameduplicate = "";
		final String name = names.get(names.size()-1);
		try {
			// Read the file
			final BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
			while ((line = bufferedReader.readLine()) != null) {
				// For each line that contains the pathname
				if (line.contains(pathname)) {
					final String[] split = line.split("@");
					String filename;

					// Get the child after the pathname
					final String child = split[0].substring(split[0].lastIndexOf(name) + name.length())
							.trim();
					
					if (child == null || child.equals("") || child.equals("/")) {
						// If child is null, it means it is a file
						continue;
					} 
					else if (child.contains("/")) {
						// If a directory get the next child
						filename = child.split("/")[1];
					} else {
						// Else, just get the file
						filename = child;
					}

					if (filenameduplicate.equalsIgnoreCase(filename)) {
						continue;
					}

					// Figure out if child is a file or directory and write output
					filenameduplicate = filename;
					if (line.contains("/@")) {
						output.addRow("Directory", filename);
					} else {
						output.addRow("File", filename);					}
				}
			}

			// Error on path as a file and if directory is empty
			bufferedReader.close();
			if (filenameduplicate.equalsIgnoreCase("")) {
				throw new InvalidDataException(filePath + " is a file. Expecting directory!");
			}
			if (filenameduplicate.equalsIgnoreCase("/")) {
				throw new InvalidDataException("Directory is empty");
			}
		} catch (final Exception e) {
			throw new InvalidDataException(e.getMessage());
		}

		builder.appendContent(output.toString());
		return builder;
		
		
	}

	public Message lsl(final Directory root,
			 String filePath,
			final String... arguments)
					throws InvalidPropertiesFormatException,
					InvalidDataException {
		filePath = filePath.replace(" ", "-");

		String line;
		final StringBuilder pathname = new StringBuilder();
		final String[] names1 = filePath.split("/");
		final List<String> names = new ArrayList<String>();
			names.addAll(Arrays.asList(names1));
		final String path = getPath(filePath, pathname, names, names.size() - 1);
		
		
		final File file = new File(Paths.get(NFS_FOLDER, path + ".txt").toString());
	
		final Message builder = new Message("Listing for " + filePath + "\n");
		final OutputFormatter output = new OutputFormatter();
		output.addRow("TYPE", "NAME" , "SIZE" , "TIMESTAMP");

		String filenameduplicate = "";
		final String name = names.get(names.size()-1);
		try {
			// Read the file
			final BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
			while ((line = bufferedReader.readLine()) != null) {
				// For each line that contains the pathname
				if (line.contains(pathname)) {
					final String[] split = line.split("@");
					String filename;
					String size = split[1];
					String timestamp = split[2];

					// Get the child after the pathname
					final String child = split[0].substring(split[0].lastIndexOf(name) + name.length())
							.trim();
					if (child == null || child.equals("") || child.equals("/")) {
						// If child is null, it means it is a file
						continue;
					}  else if (child.contains("/")) {
						// If a directory get the next child
						filename = child.split("/")[1];
					} else {
						// Else, just get the file
						filename = child;
					}

					if (filenameduplicate.equalsIgnoreCase(filename)) {
						continue;
					}

					// Figure out if child is a file or directory and write output
					filenameduplicate = filename;
					if (line.contains("/@")) {
						output.addRow("Directory", filename, "Size" , size , "Timestamp" , timestamp);
					} else {
						output.addRow("File", filename, "Size" , size , "Timestamp" , timestamp);
					}
				}
			}

			// Error on path as a file and if directory is empty
			bufferedReader.close();
			if (filenameduplicate.equalsIgnoreCase("")) {
				throw new InvalidDataException(filePath + " is a file. Expecting directory!");
			}
			if (filenameduplicate.equalsIgnoreCase("/")) {
				throw new InvalidDataException("Directory is empty");
			}
		} catch (final Exception e) {
			throw new InvalidDataException(e.getMessage());
		}

		builder.appendContent(output.toString());
		return builder;
		
		
	}

	@Override
	public Message mkdir(final Directory root,
			final String filepath,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		
		final Directory directory = new Directory("", false, null, System.currentTimeMillis(), (long) 4096);
		
		final List<String> names = new ArrayList<String>();
		final String filePath = getFilePath(filepath, names);

		StringBuilder pathname = new StringBuilder();

		final String path = getPath( filePath,pathname,names,names.size() - 2) ;
		
		File file = new File(Paths.get(NFS_FOLDER, path + ".txt").toString());
		
		if (!file.exists()) {
			throw new InvalidPathException(filePath, "Does not exist");
		}
		
		BufferedReader reader;
		String currentLine;
		String linename = pathname + "/"+ names.get(names.size()-1) + "/";
	
		try {
			reader = new BufferedReader(new FileReader(file));
			while ((currentLine = reader.readLine()) != null) {
				String trimmedLine = currentLine.trim();
				if (trimmedLine.contains(linename)) {	
					reader.close();
					throw new InvalidPathException(filePath, " directory already present");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		String filename = "";
		File levelfile = null;
		if (filePath.split("/").length % CUT_LEVEL == 0) {
			for (int l = 0; l < names.size(); l++) {
				if (l == names.size() - 1) {
					filename = filename.concat(names.get(l));
					break;
				}

				filename = filename.concat(names.get(l));
				filename = filename.concat("+");
			}

			levelfile = new File(NFS_FOLDER + "/" + filename + ".txt");
			appendRecord(directory, names.get(names.size() - 1) + "/", levelfile);
		}

		appendRecord(directory, pathname + "/" + names.get(names.size() - 1) + "/", file);
		return new Message("MKDIR Successful");
	}

	@Override
	public Message touch(final Directory root,
			 String filepath,
			final String... arguments)
					throws InvalidPropertiesFormatException {
				filepath = filepath.replace(" ", "-");

		final StringBuilder pathname = new StringBuilder();
		List<String> names = new ArrayList<>();
		final String filePath = getFilePath(filepath, names);
		
		final String dhtPath = getPath(filePath, pathname, names, names.size() - 2);
		File file = new File(Paths.get(NFS_FOLDER, dhtPath + ".txt").toString());
		
		if (!file.exists()) {
			throw new InvalidPathException(filePath, "Does not exist");
		}
		PrintWriter writer;
		BufferedReader reader;
		int counter = 0, counter1 = 0;
		String line = "", currentLine = "";
		final File temp = new File(file.getAbsolutePath() + ".tmp");
		try {
			reader = new BufferedReader(new FileReader(file));
			writer = new PrintWriter(new BufferedWriter(new FileWriter(temp)));
			while ((currentLine = reader.readLine()) != null) {
				String trimmedLine = currentLine.trim();
				if (trimmedLine.contains(pathname)) {
					if (counter == 0) {
						line = trimmedLine;
						counter++;
					}

					if (trimmedLine.contains(pathname + "/" + names.get(names.size() - 1))) {
						counter1++;
						final String[] split = trimmedLine.split("@");
						trimmedLine = "";
						split[2] = Long.toString(System.currentTimeMillis());

						for (int b = 0; b <= 2; b++) {
							if (b == 2) {
								trimmedLine = trimmedLine.concat(split[b]);
								break;
							}

							trimmedLine = trimmedLine.concat(split[b]);
							trimmedLine = trimmedLine.concat("@");
						}
					}
				}

				writer.print(trimmedLine);
				writer.println();
				writer.flush();
			}
			if (counter1 == 0) {
				final String[] split = line.split("@");
				if (split[0].substring(split[0].length() - 1).equals("/")) {
					split[0] = split[0].concat("/" + names.get(names.size() - 1));
					split[1] = "0";
					split[2] = Long.toString(System.currentTimeMillis());
					for (int c = 0; c <= 2; c++) {
						if (c == 2) {
							line = line.concat(split[c]);
							break;
						}
						line = line.concat(split[c]);
						line = line.concat("@");
					}

					writer.print(line);
					writer.println();
					writer.close();
				} else {
					writer.close();
					reader.close();
					throw new InvalidPathException(filePath, "cannot create a file inside a file");
				}
			}

			reader.close();
		} catch (final IOException e) {
			throw new InvalidPropertiesFormatException("Failed to modify metadata in local");
		}
		return new Message("Touch Successful");
		
	}

//	public Message rmdir(final Directory root,
//			final String filepath,
//			final String... arguments){
//		int counter = 0;
//		BufferedReader reader;
//		PrintWriter writer;
//		String currentLine;
//	
//		StringBuilder pathname = new StringBuilder();
//		List<String> names = new ArrayList<>();
////
////		if (filepath.contains("/")) {
////			names.addAll(Arrays.asList(filepath.split("/")));
////		} else {
////			names.add(filepath);
////		}		
//		final String filePath = getFilePath(filepath, names);
//		final String path ;
//		String linename;
//		
//		if((names.size()-1) % CUT_LEVEL == 0){
//			 path = getPath( filepath,pathname,names,names.size() - 1) ;
//			 linename = pathname.toString();
//			 
//		}
//		else{
//			path = getPath( filePath,pathname,names,names.size() - 2) ;
//			linename = pathname + "/"+ names.get(names.size()-1) + "/";
//
//		}
//		
//		File file = new File(Paths.get(NFS_FOLDER, path + ".txt").toString());
//
//		if (!file.exists()) {
//			throw new InvalidPathException(filePath, "Does not exist");
//		}
//		
//		final File temp = new File(file.getAbsolutePath() + ".tmp");
//
//		try {
//			reader = new BufferedReader(new FileReader(file));
//			writer = new PrintWriter(new BufferedWriter(new FileWriter(temp)));
//			
//
//
//			while ((currentLine = reader.readLine()) != null) {
//				final String trimmedLine = currentLine.trim();
//				
//				if (trimmedLine.contains(linename)) {
//					counter++;
//					
//					if(counter == 2 ){
//						temp.delete();
//						writer.close();
//						reader.close();
//						throw new InvalidPathException(filepath, "contains files");
//					}
//						
//					continue;
//				}
//				writer.println(currentLine);
//			}
//			
//			if(counter == 0){
//				temp.delete();
//				writer.close();
//				reader.close();
//				throw new InvalidPathException(filepath, "is not a valid directory");
//			}
//			else{
//			file.delete();
//			writer.close();
//			reader.close();
//			
//			final boolean successful = temp.renameTo(file);
//			if (successful == false) {
//				System.out.println("not succesfully renamed");
//			}
//			}
//		}
//			catch (final IOException e) {
//				System.out.println("execption");
//			}
//		
//	return new Message("rmdir Successful");
//	}
	
	public Message rmdir(final Directory root,
			final String filePath,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		StringBuilder pathname = new StringBuilder();
		List<String> names = new ArrayList<>();

		final String filepath = getFilePath(filePath, names);
		final String path ;
		String linename;
		
//		if((names.size()-1) % CUT_LEVEL == 0){
//			 path = getPath( filepath,pathname,names,names.size() - 1) ;
//			 linename = pathname.toString();
//			 
//		}
		
			path = getPath( filepath,pathname,names,names.size() - 2) ;
			linename = pathname + "/"+ names.get(names.size()-1) + "/";

		
		
		File file = new File(Paths.get(NFS_FOLDER, path + ".txt").toString());

		if (!file.exists()) {
			throw new InvalidPathException(filePath, "Does not exist");
		}

		BufferedReader reader;
		PrintWriter writer;
		String currentLine;
		final File temp = new File(file.getAbsolutePath() + ".tmp");


		try {
			reader = new BufferedReader(new FileReader(file));
			writer = new PrintWriter(new BufferedWriter(new FileWriter(temp)));

			while ((currentLine = reader.readLine()) != null) {
				final String trimmedLine = currentLine.trim();

				if (trimmedLine.contains(linename)) {
					final String[] split = trimmedLine.split("@");

					if (split[0].matches(getRegex())) {
						
						if(split[0].matches(getRegex() + ".*/")){
							rmdir(root, split[0], arguments);

						}
						
					}

					continue;
				}
				writer.println(currentLine);
			}

			file.delete();
			writer.close();
			reader.close();
			final boolean successful = temp.renameTo(file);
			if (successful == false) {
				System.out.println("not succesfully renamed");
			}

		} catch (final IOException e) {
			System.out.println("execption");
		}
		return null;

	}

	@Override
	public void rm(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		
	}

	@Override
	public Message cd(final Directory root,
			final String filePath,
			final String... arguments)
					throws InvalidPropertiesFormatException {

		final List<String> names = new ArrayList<String>();
		final String filepath = getFilePath(filePath, names);

		StringBuilder pathname = new StringBuilder();

		final String path = getPath( filepath,pathname,names,names.size() - 2) ;
		
		File file = new File(Paths.get(NFS_FOLDER, path + ".txt").toString());
		
		if(!file.exists()){
			 throw new InvalidPathException(filePath, "Does not exist");
		}
		BufferedReader reader ;
		String currentLine;
		int counter =0;
		String line = pathname + "/"+ names.get(names.size()-1) + "/";
		 try {
			reader = new BufferedReader(new FileReader(file));
			
			while ((currentLine = reader.readLine()) != null) {
				final String trimmedLine = currentLine.trim();
				
				if (trimmedLine.contains(line)) {
					counter++;
				}
			
			}
			reader.close();
			if(counter == 0){
				
				throw new InvalidPathException(filePath, "Does not exist");
			}
		} catch (FileNotFoundException e) {
				e.printStackTrace();
		} catch (IOException e) {
				e.printStackTrace();
		}
			
		return new Message(String.valueOf(true));
	}
	
	/**
	 * Gets path in the DHT
	 *
	 * @param filePath
	 *            File path in local
	 * @param pathname
	 *            String builder to calculate path
	 * @param names
	 *            Array of directories in the path
	 * @param index
	 *            Index in the names array
	 * @return Path as string
	 */
	private String getPath(final String filePath,
			final StringBuilder pathname,
			final List<String> names,
			final int index) {

		String path;
		if (names.size() <= CUT_LEVEL) {
			pathname.append("root");
			path = pathname.toString();
		} else {
			for (int i = CUT_LEVEL - 1; i >= 0; i--) {
				if (i <= index % CUT_LEVEL) {
					if (i == 0) {
						pathname.append(names.get(index));
						break;
					}

					pathname.append(names.get(index-i));
					pathname.append("/");
				}
			}

			path = pathname.toString().split("/")[0];
			if(filePath.substring(filePath.length() - 1).equals("/")){
				final String key = filePath.substring(0, filePath.length() - pathname.length() - 1 );
				path = (key + path).replace("/", "+");
			}
			else{
			final String key = filePath.substring(0, filePath.length() - pathname.length() );
			path = (key + path).replace("/", "+");
			}
		}
		return path;
	}
	
	/**
	 * Takes in a path and gets the file path
	 *
	 * @param path
	 *            Path as string
	 * @param names
	 *            Array of path names
	 * @return File path
	 */
	private static String getFilePath(final String path,
			final List<String> names) {

		String filePath = "";
		if (path.contains("/")) {
			names.addAll(Arrays.asList(path.split("/")));
			for (int q = 0; q < names.size() - 1; q++) {
				if (q == names.size() - 2) {
					filePath = filePath.concat(names.get(q));
					break;
				}

				filePath = filePath.concat(names.get(q));
				filePath = filePath.concat("/");
			}
		} else {
			names.add(path);
			filePath = path;
		}

		return filePath;
	}


	/**
	 * Appends the {@literal file} with parameters from {@link path} and {@link directory}
	 *
	 * @param directory
	 * @param path
	 * @param file
	 */
	private static void appendRecord(final Directory directory,
			final String path,
			final File file) {
		try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
			out.print(path);
		//	out.print("@" + directory.getAccessRights());
			out.print("@" + directory.getSize());
			out.print("@" + directory.getModifiedTimeStamp());
			out.println();
			out.close();
		} catch (final Exception e) {
			LOGGER.error("File not retreived", e);
		}
	}
	
	/**
	 * Computes a regex to match the file paths
	 *
	 * @return Regex as string
	 */
	private static String getRegex() {
		String regexshort = "";
		for (int s = 0; s < CUT_LEVEL; s++) {
			if (s == CUT_LEVEL - 1) {
				regexshort = regexshort.concat(".*");
				break;
			}
			regexshort = regexshort.concat(".*/");
		}
		return regexshort;
	}

	@Override
	public Directory releaseParentReadLocks(final Directory root,final String filePath) {
				return null;
	}

	@Override
	public Message acquireReadLocks(Directory root, String filePath, String... arguments) {
				return null;
	}

	@Override
	public Message acquireWriteLocks(Directory root, String filePath, String... arguments) {
				return null;
	}

	@Override
	public Message releaseReadLocks(Directory root, String filePath, String... arguments) {
				return null;
	}

	@Override
	public Message releaseWriteLocks(Directory root, String filePath, String... arguments) {
				return null;
	}
	
		
}