package commons.net;

import java.io.Serializable;

/**
 * IP:port address as key.
 */
public class Address implements Serializable {
	private final String	ip;
	private final int		port;

	public Address(final String ip, final int port) {
		this.ip = ip;
		this.port = port;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	@Override
	public int hashCode() {
		return (ip + ":" + port).hashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof Address) {
			final Address address = (Address) obj;
			return ip.equals(address.ip) && port == address.port;
		}
		return false;
	}
}
