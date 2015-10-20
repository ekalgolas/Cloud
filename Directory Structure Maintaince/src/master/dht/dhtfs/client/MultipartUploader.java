package master.dht.dhtfs.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import master.dht.dhtfs.server.datanode.FileMeta;

public class MultipartUploader {

    private List<DataUploader> uploaders;

    public MultipartUploader() {
        uploaders = new ArrayList<DataUploader>();
    }

    public void register(ByteBuffer buf, FileMeta meta, long pos,
            Set<Integer> updated) {
        long offset = 0;
        int len = buf.limit() - buf.position();

        for (int i = 0; i < meta.getBlkNum(); ++i) {
            if (offset + meta.getBlkSizes().get(i) <= pos) {
                offset += meta.getBlkSizes().get(i);
                continue;
            }
            if (offset >= pos + len) {
                break;
            }
            ByteBuffer buffer = buf.slice();
            buffer.position((int) (offset + Math.max(pos - offset, 0) - pos));
            int size = (int) (Math.min(meta.getBlkSizes().get(i), pos + len
                    - offset) - Math.max(pos - offset, 0));
            buffer.limit(buffer.position() + size);

            long blkVersion = updated.contains(i) ? meta.getBlkVersions()
                    .get(i) + 1 : meta.getBlkVersions().get(i);
            DataUploader uploader = new DataUploader(buffer, meta.getBlkNames()
                    .get(i), blkVersion, !updated.contains(i), (int) Math.max(
                    pos - offset, 0), meta.getBlkServers().get(i), meta
                    .getBlkLevels().get(i));
            updated.add(i);
            uploaders.add(uploader);
            offset += meta.getBlkSizes().get(i);
        }
    }

    public void upload() throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(Math
                .min(uploaders.size(), ClientConfiguration.getMaxThreadNum()));
        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < uploaders.size(); ++i) {
            futures.add(executorService.submit(uploaders.get(i)));
        }
        for (int i = 0; i < uploaders.size(); ++i) {
            try {
                futures.get(i).get();
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("download failed, status: "
                        + futures.get(i).isCancelled() + " downloader info: "
                        + uploaders.get(i).toString());
            }
        }
        executorService.shutdown();
    }
}
