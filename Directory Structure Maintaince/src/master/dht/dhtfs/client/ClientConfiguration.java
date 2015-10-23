package master.dht.dhtfs.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ClientConfiguration {

    private static int reqWindowSize = 4;
    private static int msgBufferSize = 1 << 16;
    private static int maxThreadNum = 4;
    private static double latitude = 0.0;
    private static double longitude = 0.0;
    private static String masterIp;
    private static int masterPort;
    private static int preferredBlkSize;

    public static void initialize(String config) throws IOException {
        Properties properties = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(config);
        } catch (FileNotFoundException e) {
            throw new IOException(e.getMessage(), e);
        }
        properties.load(fis);
        if (properties.getProperty("reqWindowSize") != null) {
            reqWindowSize = Integer.parseInt(properties
                    .getProperty("reqWindowSize"));
        }
        if (properties.getProperty("msgBufferSize") != null) {
            msgBufferSize = Integer.parseInt(properties
                    .getProperty("msgBufferSize"));
        }
        if (properties.getProperty("maxThreadNum") != null) {
            maxThreadNum = Integer.parseInt(properties
                    .getProperty("maxThreadNum"));
        }
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
        if (properties.getProperty("preferredBlkSize") != null) {
            preferredBlkSize = Integer.parseInt(properties
                    .getProperty("preferredBlkSize"));
        }
    }

    public static int getReqWindowSize() {
        return reqWindowSize;
    }

    public static int getMsgBufferSize() {
        return msgBufferSize;
    }

    public static int getMaxThreadNum() {
        return maxThreadNum;
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

    public static int getPreferredBlkSize() {
        return preferredBlkSize;
    }

}
