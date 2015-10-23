package master.dht.dhtfs.core.table;

import java.io.Serializable;
import java.util.ArrayList;

public class LoadBalanceInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	private int readReqPerMin;
	private int writeReqPerMin;
	private ArrayList<Long> readBlock;
	private ArrayList<Long> writeBlock;
	private int diskUtilization;
	private int memoryUtilization;
	private int cpuUtilization;
	private int activeTCPConnections;
	private int noOfMeta;
	private ArrayList<Long> readMeta;
	private ArrayList<Long> writeMeta;

	public int getReadReqPerMin() {
		return readReqPerMin;
	}

	public int getWriteReqPerMin() {
		return writeReqPerMin;
	}

	public ArrayList<Long> getReadBlock() {
		return readBlock;
	}

	public ArrayList<Long> getWriteBlock() {
		return writeBlock;
	}

	public int getDiskUtilization() {
		return diskUtilization;
	}

	public int getMemoryUtilization() {
		return memoryUtilization;
	}

	public int getCpuUtilization() {
		return cpuUtilization;
	}

	public int getActiveTCPConnections() {
		return activeTCPConnections;
	}

	public int getNoOfMeta() {
		return noOfMeta;
	}

	public ArrayList<Long> getReadMeta() {
		return readMeta;
	}

	public ArrayList<Long> getWriteMeta() {
		return writeMeta;
	}

	public void setReadReqPerMin(int readReqPerMin) {
		this.readReqPerMin = readReqPerMin;
	}

	public void setWriteReqPerMin(int writeReqPerMin) {
		this.writeReqPerMin = writeReqPerMin;
	}

	public void setReadBlock(ArrayList<Long> readBlock) {
		this.readBlock = readBlock;
	}

	public void setWriteBlock(ArrayList<Long> writeBlock) {
		this.writeBlock = writeBlock;
	}

	public void setDiskUtilization(int diskUtilization) {
		this.diskUtilization = diskUtilization;
	}

	public void setMemoryUtilization(int memoryUtilization) {
		this.memoryUtilization = memoryUtilization;
	}

	public void setCpuUtilization(int cpuUtilization) {
		this.cpuUtilization = cpuUtilization;
	}

	public void setActiveTCPConnections(int activeTCPConnections) {
		this.activeTCPConnections = activeTCPConnections;
	}

	public void setNoOfMeta(int noOfMeta) {
		this.noOfMeta = noOfMeta;
	}

	public void setReadMeta(ArrayList<Long> readMeta) {
		this.readMeta = readMeta;
	}

	public void setWriteMeta(ArrayList<Long> writeMeta) {
		this.writeMeta = writeMeta;
	}
}
