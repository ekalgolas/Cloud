package master.dht.dhtfs.server.datanode;

import java.util.HashSet;

import master.dht.dhtfs.core.Saveable;

public class FileInfo extends Saveable {
    private static final long serialVersionUID = 1L;

    private boolean isFile;
    private String fileName;
    private long fileSize;
    private HashSet<FileInfo> fileInfos;

    public FileInfo(String fileName) {
        isFile = false;
        this.fileName = fileName;
        fileInfos = new HashSet<FileInfo>();
    }

    public FileInfo() {
        fileInfos = new HashSet<FileInfo>();
    }

    public boolean equals(Object info) {
        return (info instanceof FileInfo) && (info != null)
                && fileName.equals(((FileInfo) info).getFileName());
    }

    public int hashCode() {
        return fileName.hashCode();
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

    public boolean isFile() {
        return isFile;
    }

    public void setFile(boolean isFile) {
        this.isFile = isFile;
    }

    public HashSet<FileInfo> getFileInfos() {
        return fileInfos;
    }

    public void setFileInfos(HashSet<FileInfo> fileInfos) {
        this.fileInfos = fileInfos;
    }

}
