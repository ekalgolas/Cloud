package master.metadata;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Contains the inode information of a directory/file.
 * @author jaykay
 */
public class Inode implements Serializable{
	
	/**
	 * Generated serial version UID 
	 */
	private static final long serialVersionUID = -7343757662271383355L;

	/**
	 * Unique ID for directory/file across cluster. 
	 * Note: This should be left empty if the directory resides in another MDS.
	 */
	private Long inodeNumber;
	
	/**
	 *  List of MDS containing this directory/file. 
	 */
	private ArrayList<MetaDataServerInfo> dataServerInfo;
	
	/**
	 * Constructor for inode with MDS list initialiser.
	 */
	public Inode() {
		this.dataServerInfo = new ArrayList<>();
	}
	
	/**
	 * Get the unique inode number.
	 * @return inodeNumber
	 */
	public Long getInodeNumber() {
		return inodeNumber;
	}
	/**
	 * Set the inode number.
	 * @param inodeNumber
	 */
	public void setInodeNumber(Long inodeNumber) {
		this.inodeNumber = inodeNumber;
	}
	/**
	 * Get the list of MDS containing this directory/file 
	 * @return MDS list
	 */
	public ArrayList<MetaDataServerInfo> getDataServerInfo() {
		return dataServerInfo;
	}

	@Override
	public String toString() {
		return "Inode [inodeNumber=" + inodeNumber + ", dataServerInfo=" + dataServerInfo + "]";
	}
		
}
