package dht.nio.protocol.meta;

import java.util.List;

import dht.nio.protocol.ProtocolReq;
import dht.nio.protocol.ReqType;

public class CommitFileReq extends ProtocolReq {

	private static final long serialVersionUID = 1L;

	private String fileName;// file name, "/username/pic/scene.jpg"
	private long fileSize;
	private int blkNum;
	private List<Long> blkVersions;// suffix of block names
	private List<Long> blkSizes;
	private List<String> blkNames;// block base name without version number
	private List<String> blkCheckSums;// check sum for blocks

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
		System.out.println("***********END***********");
	}

	public CommitFileReq(ReqType requestType) {
		super(requestType);
	}

	public CommitFileReq(int rId, ReqType requestType) {
		super(rId, requestType);
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

}
