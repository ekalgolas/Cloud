package master.dht.dhtfs.server.datanode;

import java.util.ArrayList;
import java.util.List;

import master.dht.dhtfs.core.Saveable;
import master.dht.dhtfs.core.table.PhysicalNode;

public class FileMeta extends Saveable {

    private static final long serialVersionUID = 1L;

    private long version;
    private String fileName;
    private long fileSize;
    private int blkNum;
    private List<Long> blkVersions;
    private List<Integer> blkSizes;
    private List<String> blkNames;
    private List<String> blkCheckSums;
    private List<String> blkLocks;

    private List<List<PhysicalNode>> blkServers;
    private List<List<Integer>> blkLevels;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("fileName: " + fileName + "\n"
                + "fileSize: " + fileSize + "\n" + "blkNum: " + blkNum + "\n");
        for (int i = 0; i < blkNum; ++i) {
            sb.append("\t" + blkNames.get(i) + " " + blkVersions.get(i) + " "
                    + blkSizes.get(i) + "\n");
        }
        sb.append("\n");
        return sb.toString();
    }

    public void dump() {
        System.out.println(toString());
    }

    public FileMeta(String name) {
        version = 0;
        fileName = name;
        fileSize = 0;
        blkNum = 0;
        blkVersions = new ArrayList<Long>();
        blkSizes = new ArrayList<Integer>();
        blkNames = new ArrayList<String>();
        blkCheckSums = new ArrayList<String>();
        blkServers = new ArrayList<List<PhysicalNode>>();
        blkLevels = new ArrayList<List<Integer>>();
        blkLocks = new ArrayList<String>();
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

    public List<Integer> getBlkSizes() {
        return blkSizes;
    }

    public void setBlkSizes(List<Integer> blkSizes) {
        this.blkSizes = blkSizes;
    }

    public List<Long> getBlkVersions() {
        return blkVersions;
    }

    public void setBlkVersions(List<Long> blkVersions) {
        this.blkVersions = blkVersions;
    }

    public List<List<PhysicalNode>> getBlkServers() {
        return blkServers;
    }

    public void setBlkServers(List<List<PhysicalNode>> blkServers) {
        this.blkServers = blkServers;
    }

    public List<List<Integer>> getBlkLevels() {
        return blkLevels;
    }

    public void setBlkLevels(List<List<Integer>> blkLevels) {
        this.blkLevels = blkLevels;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public List<String> getBlkLocks() {
        return blkLocks;
    }

    public void setBlkLocks(List<String> blkLocks) {
        this.blkLocks = blkLocks;
    }

}
