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

public class MultipartDownloader {

	private List<DataDownloader> downloaders;

	public MultipartDownloader() {
		downloaders = new ArrayList<DataDownloader>();
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
			int val = updated.contains(i) ? 1 : 0;
			DataDownloader downloader = new DataDownloader(buffer, meta
					.getBlkNames().get(i), meta.getBlkVersions().get(i) + val,
					(int) Math.max(pos - offset, 0), meta.getBlkServers()
							.get(i), meta.getBlkLevels().get(i));
			downloaders.add(downloader);
			offset += meta.getBlkSizes().get(i);
		}
	}

	public void download() throws IOException {
		if (downloaders.size() == 0) {
			// System.out.println("nothing to be download");
			return;
		}
		ExecutorService executorService = Executors
				.newFixedThreadPool(Math.min(downloaders.size(),
						ClientConfiguration.getMaxThreadNum()));
		List<Future> futures = new ArrayList<Future>();
		for (int i = 0; i < downloaders.size(); ++i) {
			futures.add(executorService.submit(downloaders.get(i)));
		}
		for (int i = 0; i < downloaders.size(); ++i) {
			try {
				futures.get(i).get();
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException("download failed, status: "
						+ futures.get(i).isCancelled() + " downloader info: "
						+ downloaders.get(i).toString());
			}
		}
		executorService.shutdown();
	}
}
