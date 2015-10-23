package master.dht.dhtfs.server.datanode;

import java.io.IOException;
import java.util.HashMap;

import master.dht.dhtfs.core.Saveable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DirMeta extends Saveable {

	Log log = LogFactory.getLog("dirMeta");
	private static final long serialVersionUID = 1L;
	private static DirMeta dir;
	private int version;
	private HashMap<String, FileInfo> files;

	public static DirMeta getInstance() throws IOException {
		if (dir == null) {
			synchronized (DirMeta.class) {
				if (dir == null) {
					try {
						dir = (DirMeta) DirMeta
								.loadMeta(DataServerConfiguration
										.getDirImageFile());
					} catch (Exception e) {
						dir = new DirMeta();
					}
				}
			}
		}
		return dir;
	}

	private DirMeta() {
		version = 0;
		files = new HashMap<String, FileInfo>();
		files.put("/", new FileInfo("/"));
	}

	public synchronized void addFile(FileInfo info) throws IOException {
		if (info.isFile()) {
			log.info("Type: AddFile Version: " + version + " FileName: "
					+ info.getFileName() + " FileSize: " + info.getFileSize());
		} else {
			log.info("Type: MkDir Version: " + version + " FileName: "
					+ info.getFileName());
		}

		String[] path = info.getFileName().split("/");
		if (path.length == 1) {
			return;
		}
		StringBuilder sb = new StringBuilder();
		String cur = "/";

		for (int i = 1; i < path.length - 1; ++i) {
			sb.append("/" + path[i]);
			String name = sb.toString();
			FileInfo curInfo = new FileInfo(name);
			if (files.get(name) != null && files.get(name).isFile()) {
				throw new IOException("path occupied by file: " + name
						+ " path: " + info.getFileName());
			}
			if (!files.containsKey(name)) {
				files.put(name, curInfo);
			}
			files.get(cur).getFileInfos().add(curInfo);
			cur = name;
		}
		files.get(cur).getFileInfos().add(info);
		files.put(info.getFileName(), info);
		++version;
	}

	public synchronized void removeFile(FileInfo info) {
		if (info.isFile()) {
			log.info("Type: RmDir Version: " + version + " FileName: "
					+ info.getFileName());
		} else {
			log.info("Type: RmFile Version: " + version + " FileName: "
					+ info.getFileName());
		}
		String fileName = info.getFileName();
		String parent = fileName.substring(0, fileName.lastIndexOf("/"));
		if (parent.equals("")) {
			parent = "/";
		}
		files.get(parent).getFileInfos().remove(info);
		files.remove(fileName);
		++version;
	}

	public FileInfo listStatus(String fileName) {
		return files.get(fileName);
	}
}
