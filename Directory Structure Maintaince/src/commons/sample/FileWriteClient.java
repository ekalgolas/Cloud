package commons.sample;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Scanner;

import commons.net.Address;
import commons.net.IOControl;
import commons.net.Session;
import commons.sample.log.Utils;
import commons.util.FileHelper;
import commons.util.Log;

public class FileWriteClient {
	private static final Log	log		= Log.get();
	static long					timeout	= 60 * 1000;	// 60 seconds

	static boolean upload(final IOControl control, final String path, final ArrayList<Address> addresses, final long position) {
		try {
			final File file = new File(path);
			final FileInputStream fis = new FileInputStream(file);
			final FileChannel src = fis.getChannel();
			final Session req = new Session(FileWriteMsgType.WRITE_CHUNK);
			final String id = file.getName();
			final long size = file.length();
			req.set("id", id);
			req.set("size", size);
			req.set("timeout", timeout);
			req.set("address", addresses);
			if (position > 0) {
				req.set("position", position);
			}
			control.send(req, addresses.get(0));
			final SocketChannel dest = req.getSocketChannel();
			FileHelper.upload(src, dest, size);
			fis.close();
			final Session result = control.get(req);
			return result.getType() == FileWriteMsgType.WRITE_OK;
		} catch (final Exception e) {
			log.w(e);
			return false;
		}
	}

	static boolean upload(final IOControl control, final String path, final ArrayList<Address> addresses) {
		return upload(control, path, addresses, 0);
	}

	static ArrayList<Address> splitAddress(final String[] tokens, final int start) {
		final ArrayList<Address> result = new ArrayList<>();
		for (int i = start; i < tokens.length; ++i) {
			final String[] parts = tokens[i].split(":");
			if (parts.length != 2) {
				return null;
			}
			try {
				final int port = Integer.parseInt(parts[1]);
				final Address address = new Address(parts[0], port);
				result.add(address);
			} catch (final NumberFormatException e) {
				return null;
			}
		}
		return result;
	}

	public static void main(final String args[]) {
		try {
			Utils.connectToLogServer(log);
			try {
				final IOControl control = new IOControl();
				// get what you type
				final Scanner in = new Scanner(System.in);
				for (;;) {
					final String cmd = in.nextLine();
					if (cmd.length() > 0) {
						final String line = cmd.trim();
						final String[] tokens = line.split("\\s");
						if (tokens.length > 1) {
							final ArrayList<Address> addresses = splitAddress(tokens, 1);
							if (addresses != null) {
								if (upload(control, tokens[0], addresses)) {
									log.i("File upload success.");
								} else {
									log.i("File upload fails.");
								}
								continue;
							}
						}
						log.i("Input local file name and list of servers");
					}
				}
			} catch (final Exception e) {
				log.w(e);
			}
		} catch (final IOException e) {
			log.w(e);
		}
	}
}
