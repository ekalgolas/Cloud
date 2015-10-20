package master.dht.dhtfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import master.dht.dhtfs.core.def.IIDAssigner;

/**
 * @author Yinzi Chen
 * @date May 6, 2014
 */
public class BlockNameAssigner implements IIDAssigner {
    private static final int defaultIncremental = 1000;
    private static final int blkIdFlush = 5;
    private static final int firstId = 10000000;
    private String uid;
    private String file;
    private AtomicLong id;
    private final long initVal;

    public BlockNameAssigner(String uid, String idFile) throws IOException {
        this.uid = uid;
        this.file = idFile;
        IncrementalLong blockId;
        if (new File(file).exists()) {
            blockId = (IncrementalLong) IncrementalLong.loadMeta(file);
            blockId.increase(blkIdFlush + defaultIncremental);
        } else {
            blockId = new IncrementalLong(firstId);
        }
        blockId.save(file);
        initVal = blockId.getCurrentId();
        id = new AtomicLong(initVal);
    }

    //
    // private long load() throws IOException {
    // FileInputStream fis = new FileInputStream(file);
    // byte[] buf = new byte[30];
    // int len = fis.read(buf);
    // String str = new String(buf, 0, len, "utf-8");
    // fis.close();
    // long val = Long.parseLong(str);
    // return val;
    // }
    //
    // synchronized private void save(long val) throws IOException {
    // FileOutputStream fos = new FileOutputStream(file);
    // fos.write(Long.toString(val).getBytes());
    // fos.close();
    // savedId = val;
    // }

    /*
     * (non-Javadoc)
     * 
     * @see master.dht.dhtfs.core.def.INameAssigner#generateUID()
     */
    @Override
    public String generateUID() throws IOException {
        long cid = id.getAndIncrement();
        if ((cid + 1 - initVal) % blkIdFlush == 0) {
            synchronized (IncrementalLong.class) {
                if (((IncrementalLong) IncrementalLong.loadMeta(file))
                        .getCurrentId() < cid + 1) {
                    IncrementalLong blockId = new IncrementalLong(cid + 1);
                    blockId.save(file);
                }
            }
        }
        return uid + "_" + cid;
    }
}
