package master.dht.nio.protocol.loadbalancing;

import java.util.ArrayList;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class LoadBalReq extends ProtocolReq {

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

	public LoadBalReq(ReqType requestType) {
		super(requestType);
	}

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

	public int getNoOfMeta() {
		return noOfMeta;
	}

	public ArrayList<Long> getReadMeta() {
		return readMeta;
	}

	public ArrayList<Long> getWriteMeta() {
		return writeMeta;
	}

	public void setReadReqPerMin(int readReqPerSec) {
		this.readReqPerMin = readReqPerSec;
	}

	public void setWriteReqPerMin(int writeReqPerSec) {
		this.writeReqPerMin = writeReqPerSec;
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

	public void setNoOfMeta(int noOfMeta) {
		this.noOfMeta = noOfMeta;
	}

	public void setReadMeta(ArrayList<Long> readMeta) {
		this.readMeta = readMeta;
	}

	public void setWriteMeta(ArrayList<Long> writeMeta) {
		this.writeMeta = writeMeta;
	}

	public int getActiveTCPConnections() {
		return activeTCPConnections;
	}

	public void setActiveTCPConnections(int activeTCPConnections) {
		this.activeTCPConnections = activeTCPConnections;
	}

}
