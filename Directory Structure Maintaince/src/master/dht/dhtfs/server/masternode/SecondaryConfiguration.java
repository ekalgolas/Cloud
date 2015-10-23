package master.dht.dhtfs.server.masternode;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class SecondaryConfiguration {
    private static int port;
    private static String imgDir;
    private static String imgFile;

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
        if (properties.getProperty("imgDir") != null) {
            imgDir = properties.getProperty("imgDir");
        }
        if (properties.getProperty("imgFile") != null) {
            imgFile = properties.getProperty("imgFile");
        }
    }

    public static int getPort() {
        return port;
    }

    public static String getImgDir() {
        return imgDir;
    }

    public static String getImgFile() {
        return imgFile;
    }

    public static void setImgFile(String imgFile) {
        SecondaryConfiguration.imgFile = imgFile;
    }

}