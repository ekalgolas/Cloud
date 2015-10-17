package dht.nio.protocol.meta;

import java.util.List;

import dht.nio.protocol.ProtocolResp;
import dht.nio.protocol.RespType;

public class OpenFileResp extends ProtocolResp {

	private static final long serialVersionUID = 1L;

	private String fileName;
	private long fileSize;
	private int blkNum;
	private List<Long> blkVersions;
	private List<Long> blkSizes;
	private List<String> blkNames;
	private List<String> blkCheckSums;
	private List<String> newBlkNames;// names of the new blocks to write data

	public void dump() {
		System.out.println("***********BEGIN***********");
		super.dump();
		System.out.println("fileName: " + fileName);
		System.out.println("fileSize: " + fileSize);
		System.out.println("blkNum: " + blkNum);
		dumpLong("blkVersions", blkVersions);
		dumpLong("blkSizes", blkSizes);
		dumpStr("blkNames", blkNames);
		dumpStr("blkCheckSums", blkCheckSums);
		dumpStr("newBlkNames", newBlkNames);
		System.out.println("***********END***********");
	}

	public OpenFileResp(RespType responseType) {
		super(responseType);
	}

	public OpenFileResp(int rId, RespType responseType) {
		super(rId, responseType);
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public int getBlkNum() {
		return blkNum;
	}

	public void setBlkNum(int blkNum) {
		this.blkNum = blkNum;
	}

	public List<Long> getBlkSizes() {
		return blkSizes;
	}

	public void setBlkSizes(List<Long> blkSizes) {
		this.blkSizes = blkSizes;
	}

	public List<String> getBlkNames() {
		return blkNames;
	}

	public void setBlkNames(List<String> blkNames) {
		this.blkNames = blkNames;
	}

	public List<String> getBlkCheckSums() {
		return blkCheckSums;
	}

	public void setBlkCheckSums(List<String> blkCheckSums) {
		this.blkCheckSums = blkCheckSums;
	}

	public List<Long> getBlkVersions() {
		return blkVersions;
	}

	public void setBlkVersions(List<Long> blkVersions) {
		this.blkVersions = blkVersions;
	}

	public List<String> getNewBlkNames() {
		return newBlkNames;
	}

	public void setNewBlkNames(List<String> newBlkNames) {
		this.newBlkNames = newBlkNames;
	}

}
