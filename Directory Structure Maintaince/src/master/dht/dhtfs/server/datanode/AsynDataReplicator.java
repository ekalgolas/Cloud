package master.dht.dhtfs.server.datanode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import master.dht.dhtfs.core.def.IFile;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.block.WriteFileReq;
import master.dht.nio.protocol.block.WriteFinishReq;
import master.dht.nio.protocol.block.WriteFinishResp;
import master.dht.nio.protocol.block.WriteInitResp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AsynDataReplicator implements Runnable {

	Log									log	= LogFactory.getLog("blockrep");
	private Selector					selector;

	private final TransactionManager	transactionManager;
	private final Object				lock;

	public AsynDataReplicator(final TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
		lock = new Object();
	}

	public void initialize()
			throws IOException {
		selector = Selector.open();
		new Thread(this).start();
	}

	public void addReq(final String transactionId,
			final ProtocolReq req) {
		final WriteFileTransaction tran = (WriteFileTransaction) transactionManager.getTransaction(transactionId);
		// System.out.println(req.getRequestType());
		tran.getIoBuffer()
		.addOutgoing(req);
		// System.out.println(req.getRequestType() + " done");
	}

	public void register(final String transactionId)
			throws IOException {
		final WriteFileTransaction tran = (WriteFileTransaction) transactionManager.getTransaction(transactionId);
		final SocketChannel socket = SocketChannel.open();
		socket.configureBlocking(false);

		final InetSocketAddress ip = new InetSocketAddress(tran.getReplica()
				.getIpAddress(), tran.getReplica()
				.getPort());

		int opt = SelectionKey.OP_CONNECT;
		if (socket.connect(ip)) {
			opt = SelectionKey.OP_READ;
		}

		SelectionKey key;
		synchronized (lock) {
			selector.wakeup();
			key = socket.register(selector, opt);
		}

		tran.setKey(key);
		final IOBuffer<ProtocolResp, ProtocolReq> ioBuffer = new IOBuffer<ProtocolResp, ProtocolReq>(key, selector);
		tran.setIoBuffer(ioBuffer);
		key.attach(tran);

	}

	@Override
	public void run() {
		while (true) {
			synchronized (lock) {

			}
			try {
				final int count = selector.select();
				// System.out.println("SELECT: " + count);
				if (count == 0) {
					continue;
				}
				final Iterator<SelectionKey> it = selector.selectedKeys()
						.iterator();
				while (it.hasNext()) {
					final SelectionKey key = it.next();
					it.remove();
					handleKey(key);
				}
			} catch (final IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void handleKey(final SelectionKey key)
			throws IOException {
		try {
			if (key.isConnectable()) {
				// System.out.println("ASYN connectable");
				processConnect(key);
			}
			if (key.isWritable()) {
				// System.out.println("ASYN writable");
				processWrite(key);
			}
			if (key.isReadable()) {
				// System.out.println("ASYN readable");
				processRead(key);
			}
		} catch (final IOException e) {
			key.cancel();
			key.channel()
			.close();
		}
	}

	private void processRead(final SelectionKey key)
			throws IOException {
		final SocketChannel channel = (SocketChannel) key.channel();
		final WriteFileTransaction tran = (WriteFileTransaction) key.attachment();
		final IOBuffer<ProtocolResp, ProtocolReq> ioBuffer = tran.getIoBuffer();
		if (!ioBuffer.read(channel)) {
			throw new IOException("socket input stream reach the end");
			// deregister(key);
		}
		// System.out.println("connectionInfo: "
		// + info.getIp() + " " + info.getPort());
		ProtocolResp resp = null;
		// System.out.println("start to poll request @ process read");
		if ((resp = ioBuffer.pollIncoming()) != null) {
			// System.out.println("Asyn resp: " + resp.getResponseType());
			// System.out.println("outgoing size: "
			// + tran.getIoBuffer().outgoingQueue.size());
			if (resp instanceof WriteInitResp) {
				tran.setReplicaTransactionId(((WriteInitResp) resp).getTransactionId());
				// System.out.println("tranId: "
				// + ((WriteInitResp) resp).getTransactionId());
			} else if (resp instanceof WriteFinishResp) {
				// System.out.println("write finish resp");
				log.info("Type: Close Remote: " + ((SocketChannel) key.channel()).getRemoteAddress()
						.toString());
				transactionManager.removeTransaction(tran.getTransactionId());
				((WriteFinishResp) resp).getLevels()
				.add(0, tran.getLevel());
				tran.getConnectionInfo()
				.getIoBuffer()
				.addOutgoing(resp);
			}
			tran.setCanWrite(true);
			ioBuffer.setInterestOpsWrite();
		}
	}

	private void processWrite(final SelectionKey key)
			throws IOException {
		// System.err.println("process write");
		final SocketChannel socketChannel = (SocketChannel) key.channel();
		final WriteFileTransaction tran = (WriteFileTransaction) key.attachment();
		final IOBuffer<ProtocolResp, ProtocolReq> ioBuffer = tran.getIoBuffer();
		if (!tran.isCanWrite()) {
			ioBuffer.setInterestOpsRead();
			return;
		}
		ProtocolReq req = null;
		// System.err.println("process write, queuesize: "
		// + ioBuffer.outgoingQueue.size());
		if ((req = ioBuffer.pollOutgoing()) != null) {
			// if (req.getRequestType() == ReqType.WRITE_INIT) {
			//
			// } else if (req.getRequestType() == ReqType.WRITE_FILE) {
			//
			// } else if (req.getRequestType() == ReqType.WRITE_FINISH) {
			//
			// }
			final ByteArrayOutputStream bout = new ByteArrayOutputStream();
			final ObjectOutputStream out = new ObjectOutputStream(bout);
			if (req instanceof WriteFileReq) {
				final WriteFileReq fileReq = (WriteFileReq) req;
				fileReq.setTransactionId(tran.getReplicaTransactionId());
				final IFile blockFile = tran.getFile();
				final byte[] buf = new byte[fileReq.getLen()];
				synchronized (blockFile) {
					blockFile.seek(fileReq.getPos());
					blockFile.read(buf);
				}
				fileReq.setBuf(buf);
				log.info("Type: Read BlockName: " + tran.getBlkName() + " BlockVersion: " + tran.getBlkVersion() + " Pos: " + fileReq.getPos() + " Len: " +
						fileReq.getLen() + " Level: " + tran.getLevel());
			} else if (req instanceof WriteFinishReq) {
				// System.out.println("write finish req");
				((WriteFinishReq) req).setTransactionId(tran.getReplicaTransactionId());
			}
			out.writeObject(req);
			out.flush();
			// TODO optimize and share the buffer
			final ByteBuffer headBuffer = ByteBuffer.allocate(4);
			final ByteBuffer respBuffer = ByteBuffer.allocate(bout.size());
			headBuffer.putInt(bout.size());
			respBuffer.put(bout.toByteArray());
			headBuffer.flip();
			while (headBuffer.hasRemaining()) {
				socketChannel.write(headBuffer);
			}
			respBuffer.flip();
			while (respBuffer.hasRemaining()) {
				socketChannel.write(respBuffer);
			}
			bout.close();
			out.close();
			tran.setCanWrite(false);
			ioBuffer.setInterestOpsRead();
		}
	}

	private void processConnect(final SelectionKey key)
			throws IOException {
		final SocketChannel socket = (SocketChannel) key.channel();
		if (socket.finishConnect()) {
			key.interestOps(key.interestOps() ^ SelectionKey.OP_CONNECT);
			key.selector()
			.wakeup();
			log.info("Type: Connect Remote: " + socket.getRemoteAddress()
					.toString());
		}
	}
}
