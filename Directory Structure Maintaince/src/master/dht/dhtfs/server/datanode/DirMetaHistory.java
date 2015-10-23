package master.dht.dhtfs.server.datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;

public class DirMetaHistory {

    private static DirMetaHistory history;
    private PrintWriter pw;

    public static DirMetaHistory getInstance() throws IOException {
        if (history == null) {
            synchronized (DirMetaHistory.class) {
                if (history == null) {
                    history = new DirMetaHistory();
                }
            }
        }
        return history;
    }

    private DirMetaHistory() throws IOException {
        pw = new PrintWriter(DataServerConfiguration.getDirHistoryFile());
    }

    public void pushAddHistory(FileInfo info) {
        pw.println("add " + (info.isFile() ? 1 : 0) + " " + info.getFileName()
                + " " + info.getFileSize());
    }

    public void pushDeleteHistory(FileInfo info) {
        pw.println("rm " + (info.isFile() ? 1 : 0) + " " + info.getFileName());
    }

    public static void recovery(DirMeta dir) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(
                DataServerConfiguration.getDirHistoryFile()));
        String line = null;
        StringTokenizer st = null;
        while ((line = br.readLine()) != null && !line.trim().equals("")) {
            st = new StringTokenizer(line);
            String cmd = st.nextToken();
            if (cmd.equals("add")) {
                FileInfo info = new FileInfo();
                info.setFile(st.nextToken().equals("1"));
                info.setFileName(st.nextToken());
                info.setFileSize(Long.parseLong(st.nextToken()));
                dir.addFile(info);
            } else if (cmd.equals("rm")) {
                FileInfo info = new FileInfo();
                info.setFile(st.nextToken().equals("1"));
                info.setFileName(st.nextToken());
                dir.removeFile(info);
            } else {
                br.close();
                throw new IOException("unrecognized dir history: " + line);
            }
        }
        br.close();
        new File(DataServerConfiguration.getDirHistoryFile())
                .renameTo(new File(DataServerConfiguration.getDirHistoryFile()
                        + ".bak"));
    }
}
