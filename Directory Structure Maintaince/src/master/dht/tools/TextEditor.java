package master.dht.tools;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.event.CaretEvent;
import javax.swing.event.CaretListener;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import master.dht.dhtfs.client.ClientConfiguration;
import master.dht.dhtfs.client.DHTFile;
import master.dht.dhtfs.client.DHTFileSystem;

public class TextEditor extends JFrame {

    private static final String fileName = "/cyz0430/abc.txt";
    private DHTFileSystem dfs;
    private DHTFile file;

    private static final long serialVersionUID = 1L;
    private JPanel panel;
    private JPanel textPanel;
    private JPanel btnPanel;
    private JTextArea textArea;
    private JTextArea cmdArea;
    private JButton commitBtn;
    private JRadioButton modeBtn;
    private JLabel posLabel;
    private JTextField posTextField;
    private JLabel strLabel;
    private JTextField strTextField;
    private JLabel infoLabel;
    private boolean insert;

    public TextEditor(String title) throws IOException {
        super(title);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        panel = new JPanel();
        panel.setLayout(new BorderLayout());
        textArea = new JTextArea();
        textArea.setFont(new Font("Courier", Font.PLAIN, 16));
        textArea.setEditable(false);
        textArea.getCaret().setVisible(true);
        cmdArea = new JTextArea();
        cmdArea.setEditable(false);
        posLabel = new JLabel("pos:");
        posTextField = new JTextField(2);
        strLabel = new JLabel("str:");
        strTextField = new JTextField(20);
        infoLabel = new JLabel("");
        commitBtn = new JButton("Commit");
        modeBtn = new JRadioButton("Insert");
        textPanel = new JPanel();
        textPanel.setLayout(new GridLayout(2, 1));
        textPanel.add(new JScrollPane(textArea));
        textPanel.add(new JScrollPane(cmdArea));
        // textPanel.add(infoLabel, BorderLayout.PAGE_START);
        panel.add(textPanel, BorderLayout.CENTER);
        btnPanel = new JPanel();
        btnPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
        btnPanel.add(commitBtn);
        btnPanel.add(modeBtn);
        btnPanel.add(posLabel);
        btnPanel.add(posTextField);
        btnPanel.add(strLabel);
        btnPanel.add(strTextField);
        panel.add(btnPanel, BorderLayout.PAGE_END);
        add(panel);

        commitBtn.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                try {
                    save();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        });

        modeBtn.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                insert = !insert;
            }
        });

        textArea.getCaret().addChangeListener(new ChangeListener() {
            public void stateChanged(ChangeEvent e) {
                textArea.getCaret().setVisible(true); // 使Text区的文本光标显示
            }
        });

        textArea.addCaretListener(new CaretListener() {
            public void caretUpdate(CaretEvent e) {
                try {
                    int pos = textArea.getCaretPosition();
                    // int lineOfC = textArea.getLineOfOffset(pos) + 1;
                    // int col = pos - textArea.getLineStartOffset(lineOfC - 1)
                    // + 1;
                    infoLabel.setText("  Current position: " + pos);
                    posTextField.setText(Integer.toString(pos));
                } catch (Exception ex) {
                    infoLabel.setText("  Can not get current position");
                }
            }
        });
        insert = false;

        ClientConfiguration.initialize("conf/client.conf");
        dfs = new DHTFileSystem();
        dfs.initialize();
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
        cmdArea.append(file.toString());

        setSize(600, 400);
        setLocation(30, 50);
        setVisible(true);

    }

    private void save() throws IOException {
        int pos = Integer.parseInt(posTextField.getText());
        String str = strTextField.getText();
        file.seek(pos);
        StringBuilder text = new StringBuilder(textArea.getText());
        if (insert) {
            file.insert(str.getBytes());
            text.insert(pos, str);
            cmdArea.append("insert at pos: " + pos + " str: " + str + "\n");
        } else {
            file.write(str.getBytes());
            text.replace(pos, pos + str.length(), str);
            cmdArea.append("replace at pos: " + pos + " str: " + str + "\n");
        }
        textArea.setText(text.toString());
        textArea.setCaretPosition(pos);
        file.commit();
        cmdArea.append("commit succeed\n");
        cmdArea.append(file.toString());
        if (!text.toString().equals(getFile(fileName))) {
            cmdArea.append("not match\n");
        }
    }

    private String getFile(String fileName) throws IOException {
        DHTFile file = dfs.open(fileName);
        byte[] b = new byte[200];
        int len;
        StringBuilder sb = new StringBuilder();
        while ((len = file.read(b)) != -1) {
            for (int i = 0; i < len; ++i) {
                sb.append((char) b[i]);
            }
        }
        file.close();
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        new TextEditor("TextEditor");
    }

}
