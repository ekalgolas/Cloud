package dht.dhtfs.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Meta implements Serializable {
	private static final long serialVersionUID = 1L;

	public static Meta loadMeta(String metaFile) throws IOException {
		FileInputStream fis = new FileInputStream(metaFile);
		ObjectInputStream ois = new ObjectInputStream(fis);
		Meta meta = null;
		try {
			meta = (Meta) ois.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e.getMessage(), e);
		} finally {
			ois.close();
		}
		if (meta != null) {
			return meta;
		} else {
			throw new IOException("load metaFile failed, metaFile: " + metaFile);
		}
	}

	public void save(String fileName) throws IOException {
		FileOutputStream fos = new FileOutputStream(fileName);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(this);
		oos.close();
	}

	public byte[] toByteArray() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(this);
		oos.close();
		return baos.toByteArray();
	}

	public static Meta fromBytes(byte[] data) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Meta meta = null;
		try {
			meta = (Meta) ois.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e.getMessage(), e);
		} finally {
			ois.close();
		}
		return meta;
	}

	// public static Meta loadMeta(String metaFile) throws IOException {
	// FileInputStream fis = new FileInputStream(metaFile);
	// ObjectInputStream ois = new ObjectInputStream(fis);
	// Meta meta = null;
	// FileLock fl = null;
	// try {
	// fl = fis.getChannel().tryLock(0, Long.MAX_VALUE, true);
	// } catch (Exception e) {
	// ois.close();
	// throw new IOException(e.getMessage(), e);
	// }
	// if (fl != null) {
	// try {
	// meta = (Meta) ois.readObject();
	// } catch (ClassNotFoundException e) {
	// throw new IOException(e.getMessage(), e);
	// } finally {
	// fl.release();
	// ois.close();
	// }
	// }
	// if (meta != null) {
	// return meta;
	// } else {
	// throw new IOException("load metaFile failed, metaFile: " + metaFile);
	// }
	// }
	//
	// public void save(String fileName) throws IOException {
	// FileOutputStream fos = new FileOutputStream(fileName);
	// ObjectOutputStream oos = new ObjectOutputStream(fos);
	// FileLock fl = null;
	// try {
	// fl = fos.getChannel().tryLock(0, Long.MAX_VALUE, false);
	// } catch (Exception e) {
	// oos.close();
	// throw new IOException(e.getMessage(), e);
	// }
	// if (fl != null) {
	// oos.writeObject(this);
	// fl.release();
	// }
	// oos.close();
	// }
}
