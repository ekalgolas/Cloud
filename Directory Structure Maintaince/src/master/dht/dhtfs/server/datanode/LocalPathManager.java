package master.dht.dhtfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import master.dht.dhtfs.core.def.IFile;
import master.dht.dhtfs.core.def.IHashFunction;
import master.dht.dhtfs.core.table.SimpleHash;

public class LocalPathManager {
    private static final String dirPrefix = "sub_";
    private static final String blkPrefix = "blk_";
    private String rootDir;
    private int filePerDir;
    private int dirPerLevel;
    private List<AtomicLong> slotsInLevel;
    private int maxLevel;

    private static IHashFunction hashFun = new SimpleHash();

    public LocalPathManager(String rootDir, int filePerDir, int dirPerLevel) {
        this.rootDir = rootDir;
        this.filePerDir = filePerDir;
        this.dirPerLevel = dirPerLevel;
    }

    public void initialize() throws IOException {
        slotsInLevel = new ArrayList<AtomicLong>();
        long dirNum = 1;
        maxLevel = 0;
        while (dirNum < Long.MAX_VALUE / filePerDir
                && dirNum < Long.MAX_VALUE / dirPerLevel) {
            slotsInLevel.add(new AtomicLong(dirNum * filePerDir));
            dirNum *= dirPerLevel;
        }
        maxLevel = slotsInLevel.size();
        File dir = new File(rootDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        countFiles(dir, 0);
    }

    private void countFiles(File dir, int level) throws IOException {
        if (level == maxLevel) {
            return;
        }
        File[] files = dir.listFiles();
        long cnt = 0;
        for (File file : files) {
            if (file.getName().startsWith(dirPrefix)) {
                countFiles(file, level + 1);
            } else if (file.getName().startsWith(blkPrefix)) {
                --cnt;
            }
        }
        slotsInLevel.get(level).addAndGet(cnt);
    }

    public String getMappedPath(String bashName, int level) {
        StringBuilder sb = new StringBuilder(rootDir);
        int hashVal, dep = 0;
        while (dep < level) {
            sb.append(IFile.delim);
            hashVal = hashFun.hashValue(bashName, dep);
            sb.append(dirPrefix);
            sb.append(hashVal % dirPerLevel);
        }
        sb.append(IFile.delim);
        sb.append(blkPrefix);
        sb.append(bashName);
        return sb.toString();
    }

    public int assignLevel() {
        for (int i = 0; i < maxLevel; ++i) {
            long val = slotsInLevel.get(i).get();
            if (val > 0) {
                val = slotsInLevel.get(i).getAndDecrement();
                if (val > 0) {
                    return i;
                }
                slotsInLevel.get(i).incrementAndGet();
            }
        }
        return maxLevel - 1;
    }

    public void releaseLevel(int level) {
        slotsInLevel.get(level).incrementAndGet();
    }

}