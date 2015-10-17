package dht.nio.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ProtocolResp;

public class TCPConnection implements Closeable {

	Socket socket;
	DataOutputStream dos;
	DataInputStream dis;

	public void finalize() {
		try {
			close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static TCPConnection getInstance(String ip, int port) throws IOException {
		TCPConnection connection = new TCPConnection();
		connection.bind(ip, port);
		return connection;
	}

	public void bind(String ip, int port) throws IOException {
		socket = new Socket(ip, port);
		dos = null;
		dis = null;
	}

	public void request(ProtocolReq req) throws IOException {
		if (dos == null) {
			dos = new DataOutputStream(socket.getOutputStream());
		}
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(bout);
		out.writeObject(req);
		out.flush();
		dos.writeInt(bout.size());
		dos.write(bout.toByteArray());
		dos.flush();
	}

	public ProtocolResp response() throws IOException {
		if (dis == null) {
			dis = new DataInputStream(socket.getInputStream());
		}
		int head = dis.readInt();
		// System.out.println(head);
		byte[] buf = new byte[head];
		int len, off = 0;
		while (off != head) {
			len = dis.read(buf, off, head - off);
			off += len;
		}
		ByteArrayInputStream bis = new ByteArrayInputStream(buf);
		ObjectInputStream ois = new ObjectInputStream(bis);
		ProtocolResp resp = null;
		try {
			resp = (ProtocolResp) ois.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e.getMessage(), e);
		}
		return resp;
	}

	@Override
	public void close() throws IOException {
		if (dos != null) {
			dos.close();
		}
		if (dis != null) {
			dis.close();
		}
		if (socket != null) {
			socket.close();
		}
	}

}
