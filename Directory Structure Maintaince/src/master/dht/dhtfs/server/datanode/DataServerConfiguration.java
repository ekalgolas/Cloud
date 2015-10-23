package master.dht.dhtfs.server.datanode;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class DataServerConfiguration {

    private static double latitude = 0.0;
    private static double longitude = 0.0;
    private static String masterIp;
    private static int masterPort;
    private static int port;
    private static String metaDir;
    private static String dataDir;
    private static String tmpDir;
    private static String idDir;
    private static String dirImgFile;
    private static String dirHistoryFile;
    private static int filePerDir;
    private static int dirPerLevel;
    private static int dirMergeSize;
    private static String logConfigFile;
    private static boolean metaCacheOpen;

    // private static long firstBlkId = 10000000l;
    // private static int blkIdFlush = 10;

    public static void initialize(String config) throws IOException {
        Properties properties = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(config);
        } catch (FileNotFoundException e) {
            throw new IOException(e.getMessage(), e);
        }
        properties.load(fis);
        if (properties.getProperty("latitude") != null) {
            latitude = Double.parseDouble(properties.getProperty("latitude"));
        }
        if (properties.getProperty("longitude") != null) {
            longitude = Double.parseDouble(properties.getProperty("longitude"));
        }
        if (properties.getProperty("masterIp") != null) {
            masterIp = properties.getProperty("masterIp");
        }
        if (properties.getProperty("masterPort") != null) {
            masterPort = Integer.parseInt(properties.getProperty("masterPort"));
        }
        if (properties.getProperty("port") != null) {
            port = Integer.parseInt(properties.getProperty("port"));
        }
        if (properties.getProperty("metaDir") != null) {
            metaDir = properties.getProperty("metaDir");
        }
        if (properties.getProperty("dataDir") != null) {
            dataDir = properties.getProperty("dataDir");
        }
        if (properties.getProperty("tmpDir") != null) {
            tmpDir = properties.getProperty("tmpDir");
        }
        if (properties.getProperty("idDir") != null) {
            idDir = properties.getProperty("idDir");
        }
        if (properties.getProperty("dirMetaFile") != null) {
            dirImgFile = properties.getProperty("dirMetaFile");
        }
        if (properties.getProperty("dirHistoryFile") != null) {
            dirHistoryFile = properties.getProperty("dirHistoryFile");
        }
        if (properties.getProperty("filePerDir") != null) {
            setFilePerDir(Integer
                    .parseInt(properties.getProperty("filePerDir")));
        }
        if (properties.getProperty("dirPerLevel") != null) {
            setDirPerLevel(Integer.parseInt(properties
                    .getProperty("dirPerLevel")));
        }
        if (properties.getProperty("dirMergeSize") != null) {
            setDirMergeSize(Integer.parseInt(properties
                    .getProperty("dirMergeSize")));
        }
        if (properties.getProperty("logConfigFile") != null) {
            logConfigFile = properties.getProperty("logConfigFile");
        }
        if (properties.getProperty("metaCacheOpen") != null) {
            metaCacheOpen = Integer.parseInt(properties
                    .getProperty("metaCacheOpen")) != 0;
        }
    }

    public static double getLatitude() {
        return latitude;
    }

    public static double getLongitude() {
        return longitude;
    }

    public static String getMasterIp() {
        return masterIp;
    }

    public static int getMasterPort() {
        return masterPort;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        DataServerConfiguration.port = port;
    }

    public static String getDataDir() {
        return dataDir;
    }

    public static void setDataDir(String dataDir) {
        DataServerConfiguration.dataDir = dataDir;
    }

    public static String getTmpDir() {
        return tmpDir;
    }

    public static void setTmpDir(String tmpDir) {
        DataServerConfiguration.tmpDir = tmpDir;
    }

    public static String getMetaDir() {
        return metaDir;
    }

    public static void setMetaDir(String metaDir) {
        DataServerConfiguration.metaDir = metaDir;
    }

    public static String getIdDir() {
        return idDir;
    }

    public static void setIdDir(String idDir) {
        DataServerConfiguration.idDir = idDir;
    }

    public static int getFilePerDir() {
        return filePerDir;
    }

    public static void setFilePerDir(int filePerDir) {
        DataServerConfiguration.filePerDir = filePerDir;
    }

    public static int getDirPerLevel() {
        return dirPerLevel;
    }

    public static void setDirPerLevel(int dirPerLevel) {
        DataServerConfiguration.dirPerLevel = dirPerLevel;
    }

    public static String getDirImageFile() {
        return dirImgFile;
    }

    public static void setDirImageFile(String dirImageFile) {
        DataServerConfiguration.dirImgFile = dirImageFile;
    }

    public static int getDirMergeSize() {
        return dirMergeSize;
    }

    public static void setDirMergeSize(int dirMergeSize) {
        DataServerConfiguration.dirMergeSize = dirMergeSize;
    }

    public static String getLogConfigFile() {
        return logConfigFile;
    }

    public static void setLogConfigFile(String logConfigFile) {
        DataServerConfiguration.logConfigFile = logConfigFile;
    }

    public static boolean isMetaCacheOpen() {
        return metaCacheOpen;
    }

    public static void setMetaCacheOpen(boolean metaCacheOpen) {
        DataServerConfiguration.metaCacheOpen = metaCacheOpen;
    }

    public static String getDirHistoryFile() {
        return dirHistoryFile;
    }

    public static void setDirHistoryFile(String dirHistoryFile) {
        DataServerConfiguration.dirHistoryFile = dirHistoryFile;
    }

    // public static long getFirstBlkId() {
    // return firstBlkId;
    // }
    //
    // public static void setFirstBlkId(long firstBlkId) {
    // DataServerConfiguration.firstBlkId = firstBlkId;
    // }
    //
    // public static int getBlkIdFlush() {
    // return blkIdFlush;
    // }
    //
    // public static void setBlkIdFlush(int blkIdFlush) {
    // DataServerConfiguration.blkIdFlush = blkIdFlush;
    // }
}
