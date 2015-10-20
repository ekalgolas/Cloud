package master.dht.tools;

import java.io.IOException;

import master.dht.dhtfs.client.ClientConfiguration;
import master.dht.dhtfs.client.DHTFileSystem;

public class FileUploader {

    public static void main(String[] args) throws IOException {
        String confFile = "conf/client.conf";
        if (args.length < 2) {
            System.out.println("Usage: java FileUploader src dest [conf]");
            return;
        }
        if (args.length >= 3) {
            confFile = args[2];
        }
        ClientConfiguration.initialize(confFile);
        DHTFileSystem dfs = new DHTFileSystem();
        dfs.initialize();
        String src = args[0];
        String dest = args[1];
        dfs.copyFromLocal(src, dest);
        System.out.println("upload succeed");
    }

}
