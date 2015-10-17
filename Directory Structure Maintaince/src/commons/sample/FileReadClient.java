package commons.sample;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

import org.ini4j.Wini;

import commons.net.IOControl;
import commons.net.Session;
import commons.sample.log.Utils;
import commons.util.FileHelper;
import commons.util.Log;

/**
 * <pre>
 * Created by Yongtao on 9/20/2015.
 * <p/>
 * Read file from server to memory. Path is read from console
 * </pre>
 */
public class FileReadClient {
	private static final Log log = Log.get();

	static Session downloadFile(final IOControl control, final String ip, final int port, final String path,
			final long position, final long limit) throws Exception {
		final Session session = new Session(FileReadMsgType.READ_FILE);
		session.set("path", path);
		if (position > 0) {
			session.set("position", position);
		}
		if (limit > 0) {
			session.set("limit", limit);
		}
		return control.request(session, ip, port);
	}

	static String downloadToTemp(final Path tempDir, final IOControl control, final String ip, final int port,
			final String path) {
		try {
			final Session response = downloadFile(control, ip, port, path, 0, 0);
			if (response.getType() != FileReadMsgType.READ_FILE_OK) {
				return null;
			}
			final File newFile = new File(tempDir.toFile(), response.getString("name"));
			newFile.createNewFile();
			final FileOutputStream fos = new FileOutputStream(newFile);
			FileHelper.download(response.getSocketChannel(), fos.getChannel(), response.getLong("size"));
			fos.close();
			return newFile.getAbsolutePath();
		} catch (final Exception e) {
			log.w(e);
			return null;
		}
	}

	static long readFile(final IOControl control, final String ip, final int port, final String path,
			final long position, final long limit) {
		try {
			final Session response = downloadFile(control, ip, port, path, position, limit);
			if (response.getType() != FileReadMsgType.READ_FILE_OK) {
				return 0;
			}
			final long size = response.getLong("size");
			final SocketChannel src = response.getSocketChannel();
			final ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 128);
			long read = 0;
			while (read < size) {
				long read_once = 0;
				while (buffer.hasRemaining() && read < size) {
					read_once = src.read(buffer);
					if (read_once < 0) {
						break;
					}
					read += read_once;
				}
				if (read_once < 0) {
					break;
				}
				if (read < size) {
					buffer.reset();
				}
			}
			return read;
		} catch (final Exception e) {
			log.w(e);
			return 0;
		}
	}

	public static void main(final String args[]) {
		try {
			Utils.connectToLogServer(log);

			// read conf file here
			final Wini conf = new Wini(new File("conf/sample/sample.ini"));
			final String serverIP = conf.get("read server", "ip");
			final int serverPort = conf.get("read server", "port", int.class);

			try {
				final IOControl control = new IOControl();
				final Path tempDir = Files.createTempDirectory(null);
				// get what you type
				final Scanner in = new Scanner(System.in);
				final Path temp = Files.createTempDirectory(null);
				for (;;) {
					final String cmd = in.nextLine();
					if (cmd.length() > 0) {
						final String[] tokens = cmd.trim().split("\\s");
						if (tokens.length == 1) {
							// download to temp
							log.i("Down to: " + downloadToTemp(tempDir, control, serverIP, serverPort, tokens[0]));
						} else if (tokens.length == 2) {
							final String pre = tokens[0].toLowerCase();
							if (pre == "read" || pre == "r") {
								log.i("Read: " + readFile(control, serverIP, serverPort, tokens[1], 0, 0));
							} else if (pre == "download" || pre == "down" || pre == "d") {
								log.i("Down to: " + downloadToTemp(tempDir, control, serverIP, serverPort, tokens[0]));
							} else {
								log.i("False cmd format");
							}
						} else if (tokens.length == 3) {
							final String pre = tokens[0].toLowerCase();
							if (pre == "read" || pre == "r") {
								try {
									final long position = Long.parseLong(tokens[2]);
									log.i("Read: " + readFile(control, serverIP, serverPort, tokens[1], position, 0));
								} catch (final NumberFormatException e) {
									log.i("position not recognized.");
								}
							} else {
								log.i("False cmd format");
							}
						} else if (tokens.length == 4) {
							final String pre = tokens[0].toLowerCase();
							if (pre == "read" || pre == "r") {
								try {
									final long position = Long.parseLong(tokens[2]);
									final long limit = Long.parseLong(tokens[3]);
									log.i("Read: "
											+ readFile(control, serverIP, serverPort, tokens[1], position, limit));
								} catch (final NumberFormatException e) {
									log.i("position not recognized.");
								}
							} else {
								log.i("False cmd format");
							}
						} else {
							log.i("Unkown cmd.");
						}
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
