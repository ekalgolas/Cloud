package master.dht.test;

import java.io.File;

public class Test {

    public static void main(String[] args) throws Exception {
        // DataServerConfiguration.initialize("conf/datanode.conf");
        // DhtPath path = new DhtPath("/a/b/c/d/e/f");
        // String key = path.getParentKey();
        // System.out.println(key);

        File dir = new File("a/b");
        System.out.println(dir.mkdirs());
    }
}
