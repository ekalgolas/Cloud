package master.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import commons.dir.Directory;

/**
 * Contains MDS information
 *
 * @author jaykay
 */
public class MetaDataServerInfo implements Serializable {
	/**
	 * Generated serial version UID
	 */
	private static final long	serialVersionUID	= -1966660980300071736L;

	/**
	 * MDS Server Name
	 */
	private String				serverName;

	/**
	 * MDS Server IP address.
	 */
	private String				ipAddress;

	/**
	 * MDS Server type. i.e Primary/Replica
	 */
	private String				serverType;

	/**
	 * Status of the server. i.e Alive/Dead
	 */
	private String				status;

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
	public void setStatus(final String status) {
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
	public void setServerName(final String serverName) {
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
	public void setIpAddress(final String ipAddress) {
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
	public void setServerType(final String serverType) {
		this.serverType = serverType;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MetaDataServerInfo {serverName=" + serverName + ", ipAddress=" + ipAddress + ", serverType=" + serverType + ", status=" + status + "}";
	}

	/**
	 * Find the closest Directory that matches with the required file path.
	 *
	 * @param filePath
	 * @return closest directory.
	 */
	public static Directory findClosestNode(final String filePath,
			final StringBuffer matchedPath, final HashMap<String, Directory> parititionMap) {
		int maxLevel = -1;
		String maxMatchPath = "";
		for (final String node : parititionMap.keySet()) {
			// Get level for this node
			int currentLevel = 0, i = 0;
			while (i < node.length() && i < filePath.length()) {
				if (node.charAt(i) == filePath.charAt(i)) {
					if (node.charAt(i) == '/') {
						currentLevel++;
					}
				} else {
					break;
				}

				i++;
			}

			// Set max level if current level is greater
			if (currentLevel > maxLevel && i == node.length()) {
				maxLevel = currentLevel;
				maxMatchPath = node;
			}
		}

		matchedPath.append(maxMatchPath);
		return parititionMap.get(maxMatchPath);
	}
	
	/**
	 * Find the closest MDS server that matches with the required file path.
	 *
	 * @param filePath
	 * @return closest mds server.
	 */
	public static List<MetaDataServerInfo> findClosestServer(final String filePath,
			final StringBuffer matchedPath, 
			final HashMap<String,List<MetaDataServerInfo>> mdsServers) {
		int maxLevel = -1;
		String maxMatchPath = "";
		for (final String node : mdsServers.keySet()) {
			// Get level for this node
			int currentLevel = 0, i = 0;
			while (i < node.length() && i < filePath.length()) {
				if (node.charAt(i) == filePath.charAt(i)) {
					if (node.charAt(i) == '/') {
						currentLevel++;
					}
				} else {
					break;
				}

				i++;
			}

			// Set max level if current level is greater
			if (currentLevel > maxLevel && i == node.length()) {
				maxLevel = currentLevel;
				maxMatchPath = node;
			}
		}

		matchedPath.append(maxMatchPath);
		return mdsServers.get(maxMatchPath);
	}
	
	/**
	 * Convert the Metadata details in message header to List of MetaDataServerInfo
	 * @param string
	 * @return List of mds server details
	 */
	public static ArrayList<MetaDataServerInfo> fromStringToMetadata(String string) {
	    String[] strings = string.replace("[", "")
	    		.replace("]", "").replace("MetaDataServerInfo", "") .split(",  ");
	    ArrayList<MetaDataServerInfo> mdsList = new ArrayList<>();
	    for(String mds:strings)
	    {
	    	String[] properties = mds.replace("{", "").replace("}", "").split(", ");
	    	MetaDataServerInfo newMds = new MetaDataServerInfo();
	    	for(String property:properties)
	    	{
	    		String[] propertyValue = property.split("=");
	    		switch(propertyValue[0].trim())
	    		{
	    			case "serverName":
	    				newMds.setServerName(propertyValue[1].trim());
	    				break;
	    			case "ipAddress":
	    				newMds.setIpAddress(propertyValue[1].trim());
	    				break;
	    			case "serverType":
	    				newMds.setServerType(propertyValue[1].trim());
	    				break;
	    			case "status":
	    				newMds.setStatus(propertyValue[1].trim());
	    				break;
	    		}
	    	}
	    	mdsList.add(newMds);
	    }
	    return mdsList;
	 }
}