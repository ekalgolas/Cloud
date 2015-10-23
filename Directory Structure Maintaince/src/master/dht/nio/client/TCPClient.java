package master.dht.nio.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;

import master.dht.dhtfs.server.datanode.IOBuffer;
import master.dht.nio.protocol.ConnectionErrorResp;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;
import master.dht.nio.server.IController;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TCPClient implements Runnable {

    Log log = LogFactory.getLog("asynclient");
    private IController controller;
    private Selector selector;

    private Object lock;
    private HashMap<String, SelectionKey> id2Key;
    private HashMap<SelectionKey, String> key2Id;

    public TCPClient(IController controller) {
        this.controller = controller;
        lock = new Object();
        id2Key = new HashMap<String, SelectionKey>();
        key2Id = new HashMap<SelectionKey, String>();
    }

    public void initialize() throws IOException {
        selector = Selector.open();
        new Thread(this).start();
    }

    public void addReq(String uid, ProtocolReq req) {
        SelectionKey key = id2Key.get(uid);
        if (key == null) {
            return;
        }
        IOBuffer<ProtocolResp, ProtocolReq> ioBuffer = (IOBuffer<ProtocolResp, ProtocolReq>) key
                .attachment();
        ioBuffer.addOutgoing(req);
    }

    public void deregister(String uid) throws IOException {
        SelectionKey key = id2Key.get(uid);
        if (key != null) {
            key.cancel();
            key.channel().close();
            id2Key.remove(uid);
            key2Id.remove(key);
        }
    }

    public void deregister(SelectionKey key) throws IOException {
        String uid = key2Id.get(key);
        if (key != null) {
            key.cancel();
            key.channel().close();
            id2Key.remove(uid);
            key2Id.remove(key);
        }
    }

    public void register(String uid, String ip, int port) throws IOException {
        SocketChannel socket = SocketChannel.open();
        socket.configureBlocking(false);

        InetSocketAddress ipAddr = new InetSocketAddress(ip, port);

        int opt = SelectionKey.OP_CONNECT;
        if (socket.connect(ipAddr)) {
            opt = SelectionKey.OP_READ;
        }

        SelectionKey key;
        synchronized (lock) {
            selector.wakeup();
            key = socket.register(selector, opt);
        }
        IOBuffer<ProtocolResp, ProtocolReq> ioBuffer = new IOBuffer<ProtocolResp, ProtocolReq>(
                key, selector);
        key.attach(ioBuffer);
        id2Key.put(uid, key);
        key2Id.put(key, uid);
    }

    @Override
    public void run() {
        while (true) {
            synchronized (lock) {

            }
            try {
                int count = selector.select();
//                System.out.println("SELECT: " + count);
                if (count == 0) {
                    continue;
                }
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    try {
                        handleKey(key);
                    } catch (IOException e) {
                        ConnectionErrorResp resp = new ConnectionErrorResp(
                                RespType.CONNECTION_ERROR);
                        resp.setUid(key2Id.get(key));
                        controller.process(this, resp);
                        deregister(key);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleKey(SelectionKey key) throws IOException {
        try {
            if (key.isConnectable()) {
//                System.out.println("ASYN connectable");
                processConnect(key);
            }
            if (key.isWritable()) {
//                System.out.println("ASYN writable");
                processWrite(key);
            }
            if (key.isReadable()) {
//                System.out.println("ASYN readable");
                processRead(key);
            }
        } catch (IOException e) {
            key.cancel();
            key.channel().close();
        }
    }

    private void processRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        IOBuffer<ProtocolResp, ProtocolReq> ioBuffer = (IOBuffer<ProtocolResp, ProtocolReq>) key
                .attachment();
        if (!ioBuffer.read(channel)) {
            throw new IOException("socket input stream reach the end");
            // deregister(key);
        }
        // System.out.println("connectionInfo: "
        // + info.getIp() + " " + info.getPort());
        ProtocolResp resp = null;
        if ((resp = ioBuffer.pollIncoming()) != null) {
            ioBuffer.addOutgoing(controller.process(this, resp));
            ioBuffer.setInterestOpsWrite();
        }
    }

    private void processWrite(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        IOBuffer<ProtocolResp, ProtocolReq> ioBuffer = (IOBuffer<ProtocolResp, ProtocolReq>) key
                .attachment();
        ProtocolReq req = null;
        if ((req = ioBuffer.pollOutgoing()) != null) {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bout);
            out.writeObject(req);
            out.flush();
            ByteBuffer headBuffer = ByteBuffer.allocate(4);
            ByteBuffer respBuffer = ByteBuffer.allocate(bout.size());
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
            ioBuffer.setInterestOpsRead();
        }
    }

    private void processConnect(SelectionKey key) throws IOException {
        SocketChannel socket = (SocketChannel) key.channel();
        if (socket.finishConnect()) {
            key.interestOps(key.interestOps() ^ SelectionKey.OP_CONNECT);
            key.selector().wakeup();
            log.info("Type: Connect Remote: "
                    + socket.getRemoteAddress().toString());
        }
    }
}
