package dht.swift.server.datanode;

import java.util.ArrayList;
import java.util.List;

import dht.dhtfs.core.Meta;

public class FileMeta extends Meta {

	private static final long serialVersionUID = 1L;

	private String fileName;
	private long fileSize;
	private int blkNum;
	private List<Long> blkVersions;
	private List<Long> blkSizes;
	private List<String> blkNames;
	private List<String> blkCheckSums;
	private long time;

	public FileMeta(String name) {
		fileName = name;
		fileSize = 0;
		blkNum = 0;
		blkVersions = new ArrayList<Long>();
		blkSizes = new ArrayList<Long>();
		blkNames = new ArrayList<String>();
		blkCheckSums = new ArrayList<String>();
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

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
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

	public List<Long> getBlkSizes() {
		return blkSizes;
	}

	public void setBlkSizes(List<Long> blkSizes) {
		this.blkSizes = blkSizes;
	}

	public List<Long> getBlkVersions() {
		return blkVersions;
	}

	public void setBlkVersions(List<Long> blkVersions) {
		this.blkVersions = blkVersions;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	@Override
	public String toString() {
		return "FileMeta [fileName=" + fileName + ", fileSize=" + fileSize
				+ ", blkNum=" + blkNum + "]";
	}
}
