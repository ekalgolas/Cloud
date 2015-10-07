package commons.net;

import java.io.Serializable;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * <pre>
 * Created by Yongtao on 8/25/2015.
 * <p/>
 * Raw control msg between socket.
 * Java serialization is used. Though heavy and less efficient than Kryo, we can put virtually any data into header.
 * File and other large byte data should be transferred with SocketChannel.transferTo/From which utilize DMA.
 * </pre>
 */
public class Session implements Serializable {
	private static final long			serialVersionUID	= 1L;
	protected transient SocketChannel	socketChannel	= null;
	protected transient Socket			socket			= null;
	protected MsgType					type;
	protected Map<String, Serializable>	headerMap		= new ConcurrentHashMap<>();
	protected transient ExecutorService	pool;

	protected Session(final SocketChannel socketChannel, final ExecutorService pool) {
		this.socketChannel = socketChannel;
		socket = socketChannel.socket();
		this.pool = pool;
	}

	public Session(final MsgType type) {
		this.type = type;
	}

	public void copy(final Session session) {
		type = session.type;
		headerMap = session.headerMap;
	}

	@Override
	public Session clone() {
		final Session clone = new Session(getSocketChannel(), getExecutor());
		clone.copy(this);
		clone.socket = socket;
		return clone;
	}

	public MsgType getType() {
		return type;
	}

	public void setType(final MsgType type) {
		this.type = type;
	}

	public Map<String, Serializable> getHeaderMap() {
		return headerMap;
	}

	public void setHeaderMap(final Map<String, Serializable> header) {
		headerMap = header;
	}

	public Serializable get(final String key) {
		return headerMap.get(key);
	}

	public <T> T get(final String key, final Class<T> tClass) {
		return (T) headerMap.get(key);
	}

	public <T> T get(final String key, final Class<T> tClass, final T defaultVal) {
		final Serializable result = headerMap.get(key);
		return result == null ? defaultVal : (T) result;
	}

	public String getString(final String key) {
		return get(key, String.class);
	}

	public String getString(final String key, final String defaultVal) {
		return get(key, String.class, defaultVal);
	}

	public int getInt(final String key) {
		return get(key, int.class);
	}

	public int getInt(final String key, final int defaultVal) {
		return get(key, int.class, defaultVal);
	}

	public long getLong(final String key) {
		return get(key, long.class);
	}

	public long getLong(final String key, final long defaultVal) {
		return get(key, long.class, defaultVal);
	}

	public boolean getBoolean(final String key) {
		return get(key, boolean.class);
	}

	public boolean getBoolean(final String key, final boolean defaultVal) {
		return get(key, boolean.class, defaultVal);
	}

	public void set(final String key, final Serializable val) {
		headerMap.put(key, val);
	}

	public Socket getSocket() {
		return socket;
	}

	public void setSocket(final Socket socket) {
		this.socket = socket;
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}

	public void setSocketChannel(final SocketChannel socketChannel) {
		this.socketChannel = socketChannel;
	}

	public ExecutorService getExecutor() {
		return pool;
	}

	public void setExecutor(final ExecutorService pool) {
		this.pool = pool;
	}

	public Map<String, Serializable> getKeyValuePairs() {
		return headerMap;
	}

	public Address getSender() {
		return new Address(getSocket().getInetAddress()
				.getHostAddress(), getSocket().getPort());
	}
}
