package master.dht.dhtfs.server.datanode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;

public class IOBuffer<U, V> {
    int head;
    ByteBuffer headBuffer;
    ByteBuffer contentBuffer;
    Queue<U> incomingQueue;
    Queue<V> outgoingQueue;
    SelectionKey key;
    Selector selector;

    public void close() throws IOException {

    }

    public IOBuffer(SelectionKey k, Selector s) {
        head = -1;
        headBuffer = ByteBuffer.allocate(4);
        // contentBuffer = ByteBuffer.allocate(size);
        incomingQueue = new LinkedList<U>();
        outgoingQueue = new LinkedList<V>();
        key = k;
        selector = s;
    }

    public boolean read(SocketChannel channel) throws IOException {
        while (true) {
            int len;
            if (head == -1) {
                if ((len = channel.read(headBuffer)) == -1) {
                    return false;
                }
                // reactorByteReceived.addAndGet(len);
            }
            if (headBuffer.position() == 4) {
                headBuffer.flip();
                head = headBuffer.getInt();
                headBuffer.clear();
                // TODO memory improve needed
                contentBuffer = ByteBuffer.allocate(head);
//                System.out.println("head: " + head);
            }
            if (head != -1) {
                if ((len = channel.read(contentBuffer)) == -1) {
                    return false;
                }
//                System.out.println(head + ":" + contentBuffer.position());
                // reactorByteReceived.addAndGet(len);
                if (contentBuffer.position() == head) {
                    ByteArrayInputStream bais = new ByteArrayInputStream(
                            contentBuffer.array());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    U req = null;
                    try {
                        req = (U) ois.readObject();
                    } catch (ClassNotFoundException e) {
                        throw new IOException(e.getMessage(), e);
                    }
                    incomingQueue.add(req);
//                    System.out.println("add req");
                    // reactorReqNum.incrementAndGet();
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

    public U pollIncoming() {
        if (incomingQueue.isEmpty()) {
            return null;
        }
        return incomingQueue.poll();
    }

    public synchronized V pollOutgoing() {
        if (outgoingQueue.isEmpty()) {
            return null;
        }
        return outgoingQueue.poll();
    }

    public synchronized void setInterestOpsRead() {
        // if (outgoingQueue.isEmpty()) {
        key.interestOps((key.interestOps() ^ SelectionKey.OP_WRITE)
                | SelectionKey.OP_READ);
        key.selector().wakeup();
        // }
    }

    public synchronized void setInterestOpsWrite() {
        if (!outgoingQueue.isEmpty()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_READ
                    | SelectionKey.OP_WRITE);
            key.selector().wakeup();
        }
    }

    public synchronized void addOutgoing(V outgoing) {
        if (outgoing == null) {
            return;
        }
        outgoingQueue.add(outgoing);
        // synchronized (lock) {
        // selector.wakeup();
        // try {
        // Thread.sleep(3000);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        // System.out.println("interest");
        key.interestOps(key.interestOps() | SelectionKey.OP_READ
                | SelectionKey.OP_WRITE);
        // System.out.println("interested over");
        key.selector().wakeup();// I think this wakeup is needed
                                // in linux environment, but not
                                // required in Mac
        // }
    }

}