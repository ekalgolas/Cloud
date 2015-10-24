package master.dht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.InvalidPathException;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;

import com.sun.media.sound.InvalidDataException;
import commons.Globals;
import commons.Message;
import commons.dir.Directory;
import commons.dir.ICommandOperations;

public class DhtDirectoryOperations implements ICommandOperations {
	public static String ls(final HashMap<String, File> filemap, final String filePath) {
		final int N = 3;
		String line;
		int counter = 0;
		String pathname = "";
		File f = null;

		final String[] names = filePath.split("/");

		if (names.length <= N) {
			f = filemap.get(filePath);
			pathname = filePath;
		}

		else {
			for (int i = N - 1; i >= 0; i--) {
				if (i <= (names.length - 1) % N) {
					if (i == 0) {
						pathname = pathname.concat(names[names.length - 1]);
						break;
					}
					pathname = pathname.concat(names[names.length - i - 1]);
					pathname = pathname.concat("/");

				}
			}
			f = filemap.get(pathname);
		}
		if (f == null) {
			throw new InvalidPathException(filePath, "Does not exist");
		}

		final StringBuilder builder = new StringBuilder("Listing for " + filePath + "\n");
		String filenameduplicate = "";
		final String name = names[names.length - 1];
		try {
			final BufferedReader bufferedReader = new BufferedReader(new FileReader(f));

			while ((line = bufferedReader.readLine()) != null) {
				if (line.contains(pathname)) {
					final String[] split = line.split("@");
					counter++;
					if (counter == 1) {
						// break;
						continue;
					}
					String[] filename = new String[20];
					final String filename1 = split[0].substring(split[0].lastIndexOf(name) + name.length() + 1).trim();
					if (filename1.contains("/")) {
						filename = filename1.split("/");
					} else {
						filename[0] = filename1;
					}
					if (filenameduplicate.equalsIgnoreCase(filename[0])) {
						continue;
					}
					System.out.println(filename[0]);
					builder.append(filename[0] + "\n");
					filenameduplicate = filename[0];
				}
			}

			bufferedReader.close();

			if (counter == 0) {
				throw new InvalidPropertiesFormatException(filePath + " is a file. Expecting directory!");
			}

		}

		catch (final FileNotFoundException e) {
			System.out.println("file not found execption");
		} catch (final IOException e) {
			System.out.println("file IO execption");
		}

		return builder.toString();
	}

	public static void mkdir(final HashMap<String, File> filemap, final String path)
			throws InvalidPropertiesFormatException {

		final String accessRights = "drwxrwxr-x";
		final int size = 4096;
		final float readableTimeStamp = System.currentTimeMillis();
		final int N = 3;
		String pathname = "";
		File f = null;
		String[] names = new String[100];
		String filePath = "";

		if (path.contains("/")) {
			names = path.split("/");

			for (int q = 0; q < names.length - 1; q++) {
				if (q == names.length - 2) {
					filePath = filePath.concat(names[q]);
					break;
				}
				filePath = filePath.concat(names[q]);
				filePath = filePath.concat("/");
			}
		} else {
			names[0] = path;
			filePath = path;
		}

		if (names.length - 1 <= N) {
			f = filemap.get(filePath);
			pathname = filePath;
		}

		else {

			for (int i = N - 1; i >= 0; i--) {
				if (i <= (names.length - 2) % N) {
					if (i == 0) {
						pathname = pathname.concat(names[names.length - 2]);
						break;
					}
					pathname = pathname.concat(names[names.length - i - 2]);
					pathname = pathname.concat("/");

				}
			}
			f = filemap.get(pathname);
		}

		if (f == null) {
			throw new InvalidPathException(filePath, "Does not exist");
		}
		String regexshort = "";

		for (int s = 0; s < N; s++) {
			if (s == N - 1) {
				regexshort = regexshort.concat(".*");
				break;
			}
			regexshort = regexshort.concat(".*/");
		}

		PrintWriter writer;

		String filename = "";
		File levelfile = null;
		try {
			if (filePath.matches(regexshort)) {

				for (int l = 0; l < names.length; l++) {

					if (l == names.length - 1) {
						filename = filename.concat(names[l]);
						break;
					}
					filename = filename.concat(names[l]);
					filename = filename.concat(":");
				}
				levelfile = new File("/Users/sahith/Desktop/temp/" + filename + ".txt");
				writer = new PrintWriter(new BufferedWriter(new FileWriter(levelfile, true)));
				writer.print(names[names.length - 1]);
				writer.print("@" + accessRights);
				writer.print("@" + size);
				writer.print("@" + readableTimeStamp);
				writer.println();
				writer.close();

				filemap.put(names[names.length - 1], levelfile);
			}

			writer = new PrintWriter(new BufferedWriter(new FileWriter(f, true)));
			writer.print(pathname + "/" + names[names.length - 1]);
			writer.print("@" + accessRights);
			writer.print("@" + size);
			writer.print("@" + readableTimeStamp);
			writer.println();
			writer.close();

		} catch (final IOException e) {
			System.out.println("execption");
		}

	}

	public static void touch(final HashMap<String, File> filemap, final String path){

		final int N = 3;
		String pathname = "";
		File f = null;
		String[] names = new String[100];
		String filePath = "";

		if (path.contains("/")) {
			names = path.split("/");

			for (int q = 0; q < names.length - 1; q++) {
				if (q == names.length - 2) {
					filePath = filePath.concat(names[q]);
					break;
				}
				filePath = filePath.concat(names[q]);
				filePath = filePath.concat("/");
			}
		} else {
			names[0] = path;
			filePath = path;
		}

		if (names.length - 1 <= N) {
			f = filemap.get(filePath);
			pathname = filePath;
		}

		else {

			for (int i = N - 1; i >= 0; i--) {
				if (i <= (names.length - 2) % N) {
					if (i == 0) {
						pathname = pathname.concat(names[names.length - 2]);
						break;
					}
					pathname = pathname.concat(names[names.length - i - 2]);
					pathname = pathname.concat("/");

				}
			}
			f = filemap.get(pathname);
		}

		if (f == null) {
			throw new InvalidPathException(filePath, "Does not exist");
		}

		PrintWriter writer;
		BufferedReader reader;
		int counter =0;
		String line ="";
		String currentLine = "";
		final File temp = new File(f.getAbsolutePath() + ".tmp");
		try {

			reader = new BufferedReader(new FileReader(f));
			writer = new PrintWriter(new BufferedWriter(new FileWriter(temp)));

			while ((currentLine = reader.readLine()) != null) {
				String trimmedLine = currentLine.trim();

				if(trimmedLine.contains(pathname)){
					if(counter==0){
						line = trimmedLine;
						counter++;
					}

					if (trimmedLine.contains(pathname+"/"+names[names.length-1])){
						final String[] split = trimmedLine.split("@");
						split[3] = Long.toString(System.currentTimeMillis());

						for(int b=0;b<=3;b++){
							if(b==3){
								trimmedLine = trimmedLine.concat(split[b]);
							}

							trimmedLine = trimmedLine.concat(split[b]);
							trimmedLine = trimmedLine.concat("@");
						}
					}
				}
				writer.print(trimmedLine);
				writer.println();
				reader.close();
			}
			final String[] split = line.split("@");

			if(split[1].contains("d")){

				split[0] = split[0].concat("/"+names[names.length-1]);
				for(int c=0;c<=3;c++){
					if(c==3){
						line = line.concat(split[c]);
					}
					line = line.concat(split[c]);
					line = line.concat("@");
				}
				writer.print(line);
				writer.println();
				writer.close();
			}
			else{
				writer.close();
				throw new InvalidPathException(filePath, "cannot create a file inside a file");
			}
		} catch (final IOException e) {

			System.out.println("execption");
		}

	}

	public static void rmdirf(final HashMap<String, File> filemap, final String filePath)
			throws InvalidPropertiesFormatException {

		final int N = 3;
		String pathname = "";
		File f = null;
		String[] names = new String[N];

		if (filePath.contains("/")) {
			names = filePath.split("/");
		} else {
			names[0] = filePath;
		}

		if (names.length <= N) {
			f = filemap.get(filePath);
			pathname = filePath;
		}

		else {

			for (int i = N - 1; i >= 0; i--) {
				if (i <= (names.length - 1) % N) {
					if (i == 0) {
						pathname = pathname.concat(names[names.length - 1]);
						break;
					}
					pathname = pathname.concat(names[names.length - i - 1]);
					pathname = pathname.concat("/");

				}
			}
			f = filemap.get(pathname);
		}

		if (f == null) {
			throw new InvalidPathException(filePath, "Does not exist");
		}

		BufferedReader reader;
		PrintWriter writer;
		String currentLine;
		final String[] fnames = new String[1000];
		final File temp = new File(f.getAbsolutePath() + ".tmp");
		int p = 0;

		try {
			reader = new BufferedReader(new FileReader(f));
			writer = new PrintWriter(new BufferedWriter(new FileWriter(temp)));

			while ((currentLine = reader.readLine()) != null) {
				final String trimmedLine = currentLine.trim();

				if (trimmedLine.contains(pathname)) {
					final String[] split = trimmedLine.split("@");

					if (split[0].matches(".*/.*/.*/.*")) {
						final String[] name = split[0].split("/");
						fnames[p] = name[N];
						p++;

					}

					// String filename =
					// split[0].substring(split[0].lastIndexOf("/")).trim();

					// if (names.length % N == 0) {
					// File f2 = filemap.get(filename);
					// boolean removed = f2.delete();
					// if (removed)
					// System.out.println("successfully deleted the file" +
					// f2.getName());
					// }
					continue;
				}
				writer.println(currentLine);
			}
			f.delete();
			writer.close();
			reader.close();
			final boolean successful = temp.renameTo(f);
			if (successful == false) {
				System.out.println("not succesfully renamed");
			}

			for (int g = 0; g < p; g++) {

				System.out.println(g + ": " + fnames[g]);
				if (g == 64) {
					System.out.println(g);
				}

				rmdirf(filemap, fnames[g]);
			}
		} catch (final IOException e) {
			System.out.println("execption");
		}

	}

	@Override
	public Message ls(final Directory root,
			final String filePath,
			final String... arguments)
					throws InvalidPropertiesFormatException,
					InvalidDataException {
		final Message message = new Message(ls(Globals.dhtFileMap, filePath));
		return message;
	}

	@Override
	public Message mkdir(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		mkdir(Globals.dhtFileMap, path);

		final Message message = new Message("Directory successfully created");
		return message;
	}

	@Override
	public Message touch(final Directory root,
			final String path)
					throws InvalidPropertiesFormatException {
		touch(Globals.dhtFileMap, path);
		return new Message("Touch Successful");
	}

	@Override
	public void rmdir(final Directory root,
			final String path,
			final String... arguments)
					throws InvalidPropertiesFormatException {
		rmdirf(Globals.dhtFileMap, path);
	}

	@Override
	public void rm(final Directory root,
			final String path)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub

	}

	@Override
	public Message cd(final Directory root,
			final String filePath)
					throws InvalidPropertiesFormatException {
		// TODO Auto-generated method stub
		return null;
	}

}