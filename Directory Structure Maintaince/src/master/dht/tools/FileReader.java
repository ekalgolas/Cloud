package master.dht.tools;

import java.awt.Font;
import java.awt.GridLayout;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import master.dht.dhtfs.client.ClientConfiguration;
import master.dht.dhtfs.client.DHTFile;
import master.dht.dhtfs.client.DHTFileSystem;

public class FileReader extends JFrame implements Runnable {

    private DHTFileSystem dfs;
    private DHTFile file;

    private static final long serialVersionUID = 1L;
    private JPanel panel;
    private JTextArea textArea;

    public FileReader(String title) throws IOException {
        super(title);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        panel = new JPanel();
        panel.setLayout(new GridLayout());
        textArea = new JTextArea();
        textArea.setFont(new Font("Courier", Font.PLAIN, 16));
        textArea.setEditable(false);
        panel.add(new JScrollPane(textArea));
        add(panel);

        ClientConfiguration.initialize("conf/client.conf");
        dfs = new DHTFileSystem();
        dfs.initialize();

        setSize(600, 400);
        setLocation(30, 50);
        setVisible(true);
    }

    private void loadFile(String fileName) throws IOException {
        file = dfs.open(fileName);
        byte[] b = new byte[200];
        int len;
        StringBuilder sb = new StringBuilder();
        while ((len = file.read(b)) != -1) {
            for (int i = 0; i < len; ++i) {
                sb.append((char) b[i]);
            }
        }
        textArea.setText(sb.toString());
        file.close();
    }

    @Override
    public void run() {
        while (true) {
            try {
                loadFile("/cyz0430/abc.txt");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        new Thread(new FileReader("FileReader")).start();
    }

}
