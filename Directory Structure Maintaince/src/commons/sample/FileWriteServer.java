package commons.sample;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import commons.net.Address;
import commons.net.IOControl;
import commons.net.MsgHandler;
import commons.net.MsgType;
import commons.net.Session;
import commons.sample.log.Utils;
import commons.util.FileHelper;
import commons.util.Log;

/**
 * Chunk upload server using GFS scheme
 */
public class FileWriteServer {
	private static final Log log = Log.get();

	static class WriteResult {
		public Address address;
		public FileWriteMsgType result;

		WriteResult(final Address address, final FileWriteMsgType result) {
			this.address = address;
			this.result = result;
		}
	}

	static class WriteServer implements MsgHandler {
		private final IOControl control;
		private final Path chunkDir;

		WriteServer(final IOControl control, final Path chunkDir) throws IOException {
			this.control = control;
			this.chunkDir = chunkDir;
		}

		// WRITE_CHUNK, WRITE_CHUNK_CACHE
		void proc(final Session session, final boolean isPrimary, final long start) {
			final String id = session.getString("id");
			final long size = session.getLong("size");
			final long timeout = session.getLong("timeout");
			final long position = session.getLong("position", 0);
			Address primary = session.get("primary", Address.class); // nullable

			UUID transID = session.get("transid", UUID.class); // nullable
			final ArrayList<Address> addresses = session.get("address", ArrayList.class);

			final SocketChannel src = session.getSocketChannel();
			final File newChunk = new File(chunkDir.toFile(), id);
			final Session reply = session.clone();
			reply.setType(isPrimary ? FileWriteMsgType.WRITE_FAIL : FileWriteMsgType.COMMIT_FAIL);
			do {
				if (!newChunk.exists() && position > 0) {
					log.w("File not exist but position is positive");
					break;
				}
				if (isPrimary) {
					primary = addresses.remove(0);
					transID = UUID.randomUUID();
					reply.set("primary", primary);
					reply.set("transid", transID);
				} else {
					addresses.remove(0);
				}
				FileOutputStream fos = null;
				try {
					do {
						if (!newChunk.exists()) {
							newChunk.createNewFile();
						}
						fos = new FileOutputStream(newChunk);
						final FileChannel dest = fos.getChannel();
						if (addresses.size() == 0) {
							// no more forwarding
							final Future<Object> writeTrans = session.getExecutor().submit(() -> {
								FileHelper.download(src, dest, size, position);
								return null;
							});
							try {
								writeTrans.get(timeout + start - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
								fos.close();
								if (newChunk.length() == size) {
									reply.setType(isPrimary ? FileWriteMsgType.WRITE_OK : FileWriteMsgType.COMMIT_OK);
									log.i("File write to: " + newChunk.getAbsolutePath());
								}
							} catch (InterruptedException | ExecutionException | TimeoutException e) {
								log.i(e);
							}
						} else {
							// do forward
							final Session forward = reply.clone();
							forward.setType(FileWriteMsgType.WRITE_CHUNK_CACHE);
							final ArrayList<Address> commit_ok = new ArrayList<>();
							final ArrayList<Address> commit_fail = new ArrayList<>();
							final BlockingQueue<WriteResult> results = new LinkedBlockingQueue<>();
							forwardResult.put(transID, results);
							forward.set("start", start);
							control.send(forward, addresses.get(0));
							FileHelper.pipe(session.getExecutor(), src, dest, forward.getSocketChannel(), size,
									position, start, timeout);
							fos.close();
							if (newChunk.length() != size) {
								break;
							}
							log.i("File write to: " + newChunk.getAbsolutePath());
							long remain;
							while ((remain = start + timeout - System.currentTimeMillis()) >= 0) {
								final WriteResult r = results.poll(remain, TimeUnit.MILLISECONDS);
								if (r == null) {
									commit_fail.addAll(addresses);
									commit_fail.removeAll(commit_ok);
									break;
								} else {
									if (r.result == FileWriteMsgType.COMMIT_OK) {
										commit_ok.add(r.address);
									} else {
										commit_fail.add(r.address);
									}
									if (commit_fail.size() + commit_ok.size() == addresses.size()) {
										reply.setType(FileWriteMsgType.WRITE_OK);
										break;
									}
								}
							}
						}
					} while (false);
				} catch (final Exception e) {
					log.w(e);
					if (fos != null) {
						try {
							fos.close();
						} catch (final IOException ignored) {
						}
					}
				}
			} while (false);
			try {
				if (isPrimary) {
					control.response(reply, session);
				} else {
					control.send(reply, primary);
				}
			} catch (final Exception e) {
				log.w(e);
			}
		}

		private final Map<UUID, BlockingQueue<WriteResult>> forwardResult = new ConcurrentHashMap<>();

		@Override
		public boolean process(final Session session) throws IOException {
			final long start = System.currentTimeMillis();
			final MsgType type = session.getType();
			if (type == FileWriteMsgType.WRITE_CHUNK || type == FileWriteMsgType.WRITE_CHUNK_CACHE) {
				proc(session, type == FileWriteMsgType.WRITE_CHUNK, start);
			} else if (type == FileWriteMsgType.COMMIT_OK || type == FileWriteMsgType.COMMIT_FAIL) {
				final UUID transID = session.get("transid", UUID.class);
				final BlockingQueue<WriteResult> queue = forwardResult.get(transID);
				if (queue != null) {
					try {
						queue.put(new WriteResult(session.getSender(), (FileWriteMsgType) type));
					} catch (final InterruptedException ignored) {
					}
				}
			}
			return false;
		}
	}

	public static void main(final String args[]) {
		try {
			Utils.connectToLogServer(log);

			int port;
			if (args.length > 0) {
				port = Integer.parseInt(args[0]);
			} else {
				log.s("No port specified!");
				return;
			}
			final IOControl server = new IOControl();

			// register file upload handler
			// modify to your dir
			final MsgHandler fileWrite = new WriteServer(server, Files.createTempDirectory(null));

			final MsgType[] type = FileWriteMsgType.values();
			server.registerMsgFilterHead(new RawLogger());
			server.registerMsgHandlerHead(fileWrite, type);
			server.registerMsgHandlerHead(new SimpleLogger(), type);
			// start server
			server.startServer(port);
			// blocking until asked to quit (see SimpleEchoClient)
			server.waitForServer();

		} catch (final IOException e) {
			log.w(e);
		}
	}
}
