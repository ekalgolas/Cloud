package metadata;

import java.io.Serializable;

/**
 * Contains MDS information
 * 
 * @author jaykay
 *
 */
public class MetaDataServerInfo implements Serializable {

	/**
	 * Generated serial version UID
	 */
	private static final long serialVersionUID = -1966660980300071736L;
	/**
	 * MDS Server Name
	 */
	private String serverName;
	/**
	 * MDS Server IP address.
	 */
	private String ipAddress;
	/**
	 * MDS Server type. i.e Primary/Replica
	 */
	private String serverType;
	/**
	 * Status of the server. i.e Alive/Dead
	 */
	private String status;

	/**
	 * Get MDS status
	 * 
	 * @return MDS status
	 */
	public String getStatus() {
		return status;
	}

	/**
	 * Set MDS status
	 * 
	 * @param status
	 */
	public void setStatus(String status) {
		this.status = status;
	}

	/**
	 * Get MDS name
	 * 
	 * @return MDS name
	 */
	public String getServerName() {
		return serverName;
	}

	/**
	 * Set MDS name
	 * 
	 * @param serverName
	 */
	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	/**
	 * Get MDS IP Address
	 * 
	 * @return MDS IP Address
	 */
	public String getIpAddress() {
		return ipAddress;
	}

	/**
	 * Set MDS IP address
	 * 
	 * @param ipAddress
	 */
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	/**
	 * Get MDS Type
	 * 
	 * @return MDS Type
	 */
	public String getServerType() {
		return serverType;
	}

	/**
	 * Set MDS Type.
	 * 
	 * @param serverType
	 */
	public void setServerType(String serverType) {
		this.serverType = serverType;
	}

	@Override
	public String toString() {
		return "MetaDataServerInfo [serverName=" + serverName + ", ipAddress=" + ipAddress + ", serverType="
				+ serverType + ", status=" + status + "]";
	}
}
