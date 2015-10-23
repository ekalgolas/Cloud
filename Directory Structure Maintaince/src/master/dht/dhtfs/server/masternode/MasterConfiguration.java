package master.dht.dhtfs.server.masternode;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import master.dht.dhtfs.core.table.PhysicalNode;

public class MasterConfiguration {
    private static int port;
    private static int metaReplicaNum;
    private static int blockReplicaNum;
    private static int ringLength;
    private static int defaultBlockSize;
    private static int defaultSplitSize;
    private static String imgFile;
    private static String serverFile;
    private static String idSeqFile;
    private static int heartBeatPeriod;
    private static List<PhysicalNode> secondaries = new ArrayList<PhysicalNode>();

    public static void initialize(String config) throws IOException {
        Properties properties = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(config);
        } catch (FileNotFoundException e) {
            throw new IOException(e.getMessage(), e);
        }
        properties.load(fis);
        if (properties.getProperty("port") != null) {
            port = Integer.parseInt(properties.getProperty("port"));
        }
        if (properties.getProperty("metaReplicaNum") != null) {
            metaReplicaNum = Integer.parseInt(properties
                    .getProperty("metaReplicaNum"));
        }
        if (properties.getProperty("blockReplicaNum") != null) {
            blockReplicaNum = Integer.parseInt(properties
                    .getProperty("blockReplicaNum"));
        }
        if (properties.getProperty("ringLength") != null) {
            ringLength = Integer.parseInt(properties.getProperty("ringLength"));
        }
        if (properties.getProperty("defaultBlockSize") != null) {
            defaultBlockSize = Integer.parseInt(properties
                    .getProperty("defaultBlockSize"));
        }
        if (properties.getProperty("defaultSplitSize") != null) {
            defaultSplitSize = Integer.parseInt(properties
                    .getProperty("defaultSplitSize"));
        }
        if (properties.getProperty("imgFile") != null) {
            imgFile = properties.getProperty("imgFile");
        }
        if (properties.getProperty("serverFile") != null) {
            serverFile = properties.getProperty("serverFile");
        }
        if (properties.getProperty("idSeqFile") != null) {
            idSeqFile = properties.getProperty("idSeqFile");
        }
        if (properties.getProperty("heartBeatPeriod") != null) {
            setHeartBeatPeriod(Integer.parseInt(properties
                    .getProperty("heartBeatPeriod")));
        }
        if (properties.getProperty("secondaries") != null) {
            String[] str = properties.getProperty("secondaries").split(",");
            for (String s : str) {
                String[] addr = s.split(":");
                PhysicalNode node = new PhysicalNode(addr[0],
                        Integer.parseInt(addr[1]));
                secondaries.add(node);
            }
        }
    }

    public static int getPort() {
        return port;
    }

    public static int getRingLength() {
        return ringLength;
    }

    public static String getImgFile() {
        return imgFile;
    }

    public static String getServerFile() {
        return serverFile;
    }

    public static String getIdSeqFile() {
        return idSeqFile;
    }

    public static int getMetaReplicaNum() {
        return metaReplicaNum;
    }

    public static void setMetaReplicaNum(int metaReplicaNum) {
        MasterConfiguration.metaReplicaNum = metaReplicaNum;
    }

    public static int getBlockReplicaNum() {
        return blockReplicaNum;
    }

    public static void setBlockReplicaNum(int blockReplicaNum) {
        MasterConfiguration.blockReplicaNum = blockReplicaNum;
    }

    public static int getDefaultBlockSize() {
        return defaultBlockSize;
    }

    public static void setDefaultBlockSize(int defaultBlockSize) {
        MasterConfiguration.defaultBlockSize = defaultBlockSize;
    }

    public static int getDefaultSplitSize() {
        return defaultSplitSize;
    }

    public static void setDefaultSplitSize(int defaultSplitSize) {
        MasterConfiguration.defaultSplitSize = defaultSplitSize;
    }

    public static int getHeartBeatPeriod() {
        return heartBeatPeriod;
    }

    public static void setHeartBeatPeriod(int heartBeatPeriod) {
        MasterConfiguration.heartBeatPeriod = heartBeatPeriod;
    }

    public static List<PhysicalNode> getSecondaries() {
        return secondaries;
    }

    public static void setSecondaries(List<PhysicalNode> secondaries) {
        MasterConfiguration.secondaries = secondaries;
    }

}
