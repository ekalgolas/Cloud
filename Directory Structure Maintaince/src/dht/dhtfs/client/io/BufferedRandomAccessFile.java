package dht.dhtfs.client.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class BufferedRandomAccessFile extends RandomAccessFile {

	public BufferedRandomAccessFile(String name, String mode) throws FileNotFoundException {
		super(name, mode);
	}

	public int read(byte[] b, int off, int len) throws IOException {
		return super.read(b, off, len);
	}

	public void write(byte[] b, int off, int len) throws IOException {
		super.write(b, off, len);
	}
}
