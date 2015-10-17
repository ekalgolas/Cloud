package commons.util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * <pre>
 * Upload, download, pipeline file
 * From here you know how to DMA copy file directly to socket without extra memory copying.
 * </pre>
 */
public class FileHelper {
	private static final Log log = Log.get();
	private static final int bufferSize = 1024 * 256;
	private static final int queueLength = 8;

	public static void upload(final FileChannel src, final SocketChannel dest, final long size) throws IOException {
		upload(src, dest, size, 0);
	}

	public static void upload(final FileChannel src, final SocketChannel dest, final long size, long position)
			throws IOException {
		while (position < size) {
			position += src.transferTo(position, size - position, dest);
		}
	}

	public static void download(final SocketChannel src, final FileChannel dest, final long size, long position)
			throws IOException {
		final FileLock lock = dest.lock();
		try {
			while (position < size) {
				position += dest.transferFrom(src, position, size - position);
			}
		} catch (final IOException e) {
			throw e;
		} finally {
			lock.release();
		}
	}

	public static void download(final SocketChannel src, final FileChannel dest, final long size) throws IOException {
		download(src, dest, size, 0);
	}

	public static void pipe(final ExecutorService executor, final SocketChannel src, final FileChannel fDest,
			final SocketChannel dest, final long size, final long position)
					throws ExecutionException, InterruptedException, IOException {
		pipe(executor, src, fDest, dest, size, position, 0, 0);
	}

	public static void pipe(final ExecutorService executor, final SocketChannel src, final FileChannel fDest,
			final SocketChannel dest, final long size, final long position, final long start, final long timeout)
					throws ExecutionException, InterruptedException, IOException {
		final CyclicBarrier barrier = new CyclicBarrier(2);
		final GenericObjectPool<ByteBuffer> bufferRing = new GenericObjectPool<>(new ByteBufferFactory());
		final BlockingQueue<ByteBuffer> socketQueue = new ArrayBlockingQueue<>(queueLength);
		final BlockingQueue<ByteBuffer> fileQueue = new ArrayBlockingQueue<>(queueLength);
		final Reader reader = new Reader(src, size, position, bufferRing, socketQueue, fileQueue);
		final FileWriter fileWriter = new FileWriter(fDest, barrier, size, position, fileQueue);
		final SocketWriter socketWriter = new SocketWriter(dest, barrier, size, position, socketQueue, bufferRing);
		final List<Future> futures = new ArrayList<>();
		futures.add(executor.submit(reader));
		futures.add(executor.submit(socketWriter));
		futures.add(executor.submit(fileWriter));
		final FileLock lock = fDest.lock();
		try {
			if (timeout <= 0) {
				for (int i = 0; i < 3; ++i) {
					final Future future = futures.get(i);
					try {
						future.get();
					} catch (InterruptedException | ExecutionException e) {
						log.w(e);
						for (int j = i + 1; j < 3; ++j) {
							futures.get(j).cancel(true);
						}
						throw e;
					}
				}
			} else {
				int i = 0;
				while (start + timeout >= System.currentTimeMillis()) {
					try {
						futures.get(i).get();
					} catch (InterruptedException | ExecutionException e) {
						log.w(e);
						for (int j = i + 1; j < 3; ++j) {
							futures.get(j).cancel(true);
						}
						throw e;
					}
					if (++i == 3) {
						break;
					}
				}
			}
		} catch (InterruptedException | ExecutionException e) {
			throw e;
		} finally {
			lock.release();
		}
	}

	public static void pipe(final ExecutorService executor, final SocketChannel src, final FileChannel fDest,
			final SocketChannel dest, final long size) throws ExecutionException, InterruptedException, IOException {
		pipe(executor, src, fDest, dest, size, 0);
	}

	protected static class Reader implements Callable {
		private final SocketChannel src;
		private long position;
		private final long size;
		private final GenericObjectPool<ByteBuffer> bufferRing;
		private final BlockingQueue<ByteBuffer> socketQueue;
		private final BlockingQueue<ByteBuffer> fileQueue;

		Reader(final SocketChannel src, final long size, final long position,
				final GenericObjectPool<ByteBuffer> bufferRing, final BlockingQueue<ByteBuffer> socketQueue,
				final BlockingQueue<ByteBuffer> fileQueue) {
			this.src = src;
			this.size = size;
			this.position = position;
			this.bufferRing = bufferRing;
			this.socketQueue = socketQueue;
			this.fileQueue = fileQueue;
		}

		@Override
		public Object call() throws Exception {
			while (position < size) {
				final ByteBuffer buffer = bufferRing.borrowObject();
				while (buffer.hasRemaining() && position < size) {
					final long read_once = src.read(buffer);
					if (read_once < 0) {
						throw new EOFException();
					}
					position += read_once;
				}
				buffer.flip();
				final ByteBuffer fileBuffer = buffer.duplicate();
				socketQueue.put(buffer);
				fileQueue.put(fileBuffer);
			}
			return null;
		}
	}

	protected static class FileWriter implements Callable {
		private final CyclicBarrier barrier;
		private final long size;
		private long position;
		private final FileChannel fDest;
		private final BlockingQueue<ByteBuffer> fileQueue;

		FileWriter(final FileChannel fDest, final CyclicBarrier barrier, final long size, final long position,
				final BlockingQueue<ByteBuffer> fileQueue) {
			this.barrier = barrier;
			this.size = size;
			this.position = position;
			this.fDest = fDest;
			this.fileQueue = fileQueue;
		}

		@Override
		public Object call() throws Exception {
			fDest.position(position);
			while (position < size) {
				final ByteBuffer buffer = fileQueue.take();
				while (buffer.hasRemaining() && position < size) {
					position += fDest.write(buffer);
				}
				if (position < size) {
					barrier.await();
				}
			}
			return null;
		}
	}

	protected static class SocketWriter implements Callable {
		GenericObjectPool<ByteBuffer> bufferRing;
		private final CyclicBarrier barrier;
		private final long size;
		private long position;
		private final SocketChannel dest;
		private final BlockingQueue<ByteBuffer> socketQueue;

		SocketWriter(final SocketChannel dest, final CyclicBarrier barrier, final long size, final long position,
				final BlockingQueue<ByteBuffer> socketQueue, final GenericObjectPool<ByteBuffer> bufferRing) {
			this.barrier = barrier;
			this.size = size;
			this.position = position;
			this.dest = dest;
			this.socketQueue = socketQueue;
			this.bufferRing = bufferRing;
		}

		@Override
		public Object call() throws Exception {
			while (position < size) {
				final ByteBuffer buffer = socketQueue.take();
				while (buffer.hasRemaining() && position < size) {
					position += dest.write(buffer);
				}
				if (position < size) {
					barrier.await();
				}
				bufferRing.returnObject(buffer);
			}
			return null;
		}
	}

	static class ByteBufferFactory extends BasePooledObjectFactory<ByteBuffer> {
		@Override
		public void passivateObject(final PooledObject<ByteBuffer> p) throws Exception {
			super.passivateObject(p);
			p.getObject().reset();
		}

		@Override
		public ByteBuffer create() throws Exception {
			return ByteBuffer.allocateDirect(bufferSize);
		}

		@Override
		public PooledObject<ByteBuffer> wrap(final ByteBuffer obj) {
			return new DefaultPooledObject<>(obj);
		}
	}
}
