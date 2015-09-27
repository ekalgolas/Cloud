package dht.nio.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.RespType;
import dht.nio.protocol.ServerReportResp;
import dht.nio.test.SimpleProcessor;

public class TCPServer {
	protected Selector selector;
	protected ServerSocketChannel server;
	protected SelectionKey key;
	protected ReactorPool reactorPool;
	protected IProcessor processor;
	protected Acceptor acceptor;

	public TCPServer(int port, IProcessor p) throws IOException {
		processor = p;
		reactorPool = new ReactorPool(8);
		server = ServerSocketChannel.open();
		selector = Selector.open();
		server.socket().bind(new InetSocketAddress(port));
		server.configureBlocking(false);
		acceptor = new Acceptor();
		key = server.register(selector, SelectionKey.OP_ACCEPT);
	}

	public void listen() throws IOException {
		while (true) {
			int count = selector.select();
			if (count == 0) {
				continue;
			}
			Iterator<SelectionKey> it = selector.selectedKeys().iterator();
			while (it.hasNext()) {
				it.next();
				it.remove();
				acceptor.establish();
			}
		}
	}

	class Acceptor {
		public void establish() {
			try {
				if (key.isAcceptable()) {
					SocketChannel channel = server.accept();
					// System.out.println("accept");
					channel.configureBlocking(false);
					// channel.socket().setSoTimeout(1);
					reactorPool.register(channel, SelectionKey.OP_READ);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	class ReactorPool {

		protected Reactor[] reactors;
		protected Random ran;

		public ReactorPool(int size) throws IOException {
			ran = new Random();
			reactors = new Reactor[size];
			for (int i = 0; i < size; ++i) {
				reactors[i] = new Reactor(i);
				new Thread(reactors[i]).start();
			}
		}

		public int register(SocketChannel channel, int op) throws IOException {
			int val = ran.nextInt(reactors.length);
			reactors[val].register(channel, op);
			return val;
		}

		public void generateReport(ServerReportResp resp) {
			int ioReactorNum = reactors.length;
			int[] reactorConnectionNum = new int[ioReactorNum];
			long[] reactorReqNum = new long[ioReactorNum];
			long[] reactorRespNum = new long[ioReactorNum];
			long[] reactorByteReceived = new long[ioReactorNum];
			long[] reactorByteSent = new long[ioReactorNum];
			for (int i = 0; i < ioReactorNum; ++i) {
				reactorConnectionNum[i] = reactors[i].reactorConnectionNum
						.get();
				reactorReqNum[i] = reactors[i].reactorReqNum.get();
				reactorRespNum[i] = reactors[i].reactorRespNum.get();
				reactorByteReceived[i] = reactors[i].reactorByteReceived.get();
				reactorByteSent[i] = reactors[i].reactorByteSent.get();
			}
			resp.setIoReactorNum(ioReactorNum);
			resp.setReactorConnectionNum(reactorConnectionNum);
			resp.setReactorReqNum(reactorReqNum);
			resp.setReactorRespNum(reactorRespNum);
			resp.setReactorByteReceived(reactorByteReceived);
			resp.setReactorByteSent(reactorByteSent);

		}

		public class Reactor implements Runnable {

			protected int id;
			protected Selector selector;
			protected IOHandler ioHandler;
			protected Object lock;
			AtomicInteger reactorConnectionNum;
			AtomicLong reactorReqNum;
			AtomicLong reactorRespNum;
			AtomicLong reactorByteReceived;
			AtomicLong reactorByteSent;

			public Reactor(int i) throws IOException {
				id = i;
				selector = Selector.open();
				ioHandler = new IOHandler();
				lock = new Object();
				reactorConnectionNum = new AtomicInteger(0);
				reactorReqNum = new AtomicLong(0);
				reactorRespNum = new AtomicLong(0);
				reactorByteReceived = new AtomicLong(0);
				reactorByteSent = new AtomicLong(0);
			}

			public void register(SocketChannel channel, int op)
					throws IOException {
				// System.out.println("register " + id);
				synchronized (lock) {
					// System.out.println("grab lock");
					selector.wakeup();
					channel.register(selector, op);
					// System.out.println("registered " + id);
				}
				reactorConnectionNum.incrementAndGet();
			}

			public void deregister(SelectionKey key) throws IOException {
				// System.out.println("deregister: " + key.toString());
				key.cancel();
				// System.out.println("key canceled");
				key.channel().close();
				// selector.selectNow();
				// System.out.println("deregistered");
				reactorConnectionNum.decrementAndGet();
			}

			@Override
			public void run() {
				while (true) {
					try {
						// System.out.println("wait for register/interestOps");
						synchronized (lock) {
							// wait for register/interestOps to release the lock
						}
						// System.out.println("start block: " + id);
						int count = selector.select();
						// System.out.println("end block: " + count + " " + id);
						if (count == 0) {
							continue;
						}
						Iterator<SelectionKey> it = selector.selectedKeys()
								.iterator();
						while (it.hasNext()) {
							SelectionKey key = it.next();
							it.remove();
							ioHandler.handleIO(key);
							// System.out.println("handleIO finished " + id);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			class IOHandler {

				public void handleIO(SelectionKey key) throws IOException {
					try {
						if (key.isReadable()) {
							SocketChannel channel = (SocketChannel) key
									.channel();
							IOBuffer ioBuffer = (IOBuffer) key.attachment();
							if (ioBuffer == null) {
								ioBuffer = new IOBuffer(key, selector, lock);
								key.attach(ioBuffer);
							}
							if (!ioBuffer.read(channel)) {
								throw new IOException(
										"socket input stream reach the end");
								// deregister(key);
							}
							ConnectionInfo info = new ConnectionInfo();
							info.setIp(channel.socket().getInetAddress()
									.getHostAddress());
							info.setPort(channel.socket().getPort());
							// System.out.println("connectionInfo: "
							// + info.getIp() + " " + info.getPort());
							ProtocolReq req = null;
							while ((req = ioBuffer.pollRequest()) != null) {
								if (req.getRequestType() == ReqType.SERVER_REPORT) {
									// System.out.println(((ServerReportReq)
									// req)
									// .toString());
									ServerReportResp resp = new ServerReportResp(
											req.getrId(), RespType.OK);
									generateReport(resp);
									ioBuffer.addResponse(resp);
								} else {
									WorkThread worker = new WorkThread(
											processor, info, req, ioBuffer);
									ThreadPool.execute(worker);
								}
							}
						}
						if (key.isWritable()) {
							SocketChannel socketChannel = (SocketChannel) key
									.channel();
							IOBuffer ioBuffer = (IOBuffer) key.attachment();
							ProtocolResp resp = null;
							while ((resp = ioBuffer.pollResponse()) != null) {
								ByteArrayOutputStream bout = new ByteArrayOutputStream();
								ObjectOutputStream out = new ObjectOutputStream(
										bout);
								out.writeObject(resp);
								out.flush();
								// TODO optimize and share the buffer
								ByteBuffer headBuffer = ByteBuffer.allocate(4);
								ByteBuffer respBuffer = ByteBuffer
										.allocate(bout.size());
								headBuffer.putInt(bout.size());
								respBuffer.put(bout.toByteArray());
								headBuffer.flip();
								socketChannel.write(headBuffer);
								reactorByteSent
										.addAndGet(headBuffer.capacity());
								respBuffer.flip();
								socketChannel.write(respBuffer);
								reactorByteSent
										.addAndGet(respBuffer.capacity());
								reactorRespNum.incrementAndGet();
								bout.close();
								out.close();
							}
							ioBuffer.setInterestOpsRead();
							// key.interestOps(SelectionKey.OP_READ);
							// System.out.println("resp success");
						}
					} catch (Exception e) {
						IOBuffer ioBuffer = (IOBuffer) key.attachment();
						ioBuffer.close();
						deregister(key);
						// e.printStackTrace();
					}
				}

				class WorkThread implements Runnable {
					IProcessor processor;
					ConnectionInfo info;
					IOBuffer ioBuffer;
					ProtocolReq req;

					public WorkThread(IProcessor processor,
							ConnectionInfo info, ProtocolReq req,
							IOBuffer ioBuffer) {
						this.processor = processor;
						this.info = info;
						this.ioBuffer = ioBuffer;
						this.req = req;
					}

					@Override
					public void run() {
						try {
							ioBuffer.addResponse(processor.process(info, req));
						} catch (CancelledKeyException e) {
							// Socket closed by peer
						}
					}

				}

				class IOBuffer {
					int head;
					ByteBuffer headBuffer;
					ByteBuffer contentBuffer;
					Queue<ProtocolReq> reqQueue;
					Queue<ProtocolResp> respQueue;
					SelectionKey key;
					Selector selector;
					Object lock;

					public void close() throws IOException {

					}

					public IOBuffer(SelectionKey k, Selector s, Object l) {
						head = -1;
						headBuffer = ByteBuffer.allocate(4);
						// contentBuffer = ByteBuffer.allocate(size);
						reqQueue = new LinkedList<ProtocolReq>();
						respQueue = new LinkedList<ProtocolResp>();
						key = k;
						selector = s;
						lock = l;
					}

					public boolean read(SocketChannel channel)
							throws IOException {
						while (true) {
							int len;
							if (head == -1) {
								if ((len = channel.read(headBuffer)) == -1) {
									return false;
								}
								reactorByteReceived.addAndGet(len);
							}
							if (headBuffer.position() == 4) {
								headBuffer.flip();
								head = headBuffer.getInt();
								headBuffer.clear();
								// TODO memory improve needed
								contentBuffer = ByteBuffer.allocate(head);
							}
							if (head != -1) {
								if ((len = channel.read(contentBuffer)) == -1) {
									return false;
								}
								reactorByteReceived.addAndGet(len);
								if (contentBuffer.position() == head) {
									ByteArrayInputStream bais = new ByteArrayInputStream(
											contentBuffer.array());
									ObjectInputStream ois = new ObjectInputStream(
											bais);
									ProtocolReq req = null;
									try {
										req = (ProtocolReq) ois.readObject();
									} catch (ClassNotFoundException e) {
										throw new IOException(e.getMessage(), e);
									}
									reqQueue.add(req);
									reactorReqNum.incrementAndGet();
									contentBuffer.clear();
									head = -1;
								} else {
									break;
								}
							} else {
								break;
							}
						}
						return true;
					}

					public ProtocolReq pollRequest() {
						if (reqQueue.isEmpty()) {
							return null;
						}
						return reqQueue.poll();
					}

					public synchronized ProtocolResp pollResponse() {
						if (respQueue.isEmpty()) {
							return null;
						}
						return respQueue.poll();
					}

					public synchronized void setInterestOpsRead() {
						if (respQueue.isEmpty()) {
							key.interestOps(SelectionKey.OP_READ);
						}
					}

					public synchronized void addResponse(ProtocolResp resp) {
						respQueue.add(resp);
						// synchronized (lock) {
						// selector.wakeup();
						// try {
						// Thread.sleep(3000);
						// } catch (InterruptedException e) {
						// e.printStackTrace();
						// }
						// System.out.println("interest");
						key.interestOps(SelectionKey.OP_READ
								| SelectionKey.OP_WRITE);
						// System.out.println("interested over");
						key.selector().wakeup();// I think this wakeup is needed
												// in linux environment, but not
												// required in Mac
						// }
					}

				}
			}
		}
	}

	public static void main(String args[]) throws IOException {
		TCPServer server = new TCPServer(9955, new SimpleProcessor());
		server.listen();
	}
}
