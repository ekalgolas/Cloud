package dht.dhtfs.client;

import java.io.IOException;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.def.IDFSFile;
import dht.dhtfs.core.def.IFile;
import dht.dhtfs.core.def.IFileSystem;
import dht.dhtfs.core.table.PhysicalNode;
import dht.dhtfs.core.table.RouteTable;
import dht.nio.client.TCPConnection;
import dht.nio.protocol.ReqType;
import dht.nio.protocol.table.TableReq;
import dht.nio.protocol.table.TableResp;

public class DHTFileSystem implements IFileSystem {

	protected Configuration conf;
	// protected TCPClient client;
	protected RouteTable table;
	protected GeometryLocation location;
	protected PhysicalNode master;
	protected static int maxThread;

	@Override
	public void initialize(Configuration config) throws IOException {
		conf = config;
		// client = new TCPClient();
		String masterIp = conf.getProperty("masterIp");
		int masterPort = Integer.parseInt(conf.getProperty("masterPort"));
		master = new PhysicalNode(masterIp, masterPort);
		double x = Double.parseDouble(conf.getProperty("locationX"));
		double y = Double.parseDouble(conf.getProperty("locationY"));
		location = new GeometryLocation(x, y);
		maxThread = Integer.parseInt(conf.getProperty("maxThread"));
		updateRouteTable();
	}

	TCPConnection openConnection(PhysicalNode node) throws IOException {
		TCPConnection connection = TCPConnection.getInstance(node.getIpAddress(), node.getPort());
		return connection;
	}

	private void updateRouteTable() throws IOException {
		TCPConnection connection = openConnection(master);
		TableReq req = new TableReq(ReqType.TABLE);
		connection.request(req);
		TableResp resp = (TableResp) connection.response();
		connection.close();
		table = resp.getTable();
		table.dump();
		connection.close();
	}

	@Override
	public IDFSFile create(DhtPath path) throws IOException {
		return DHTFile.create(path, table, location);
	}

	@Override
	public IDFSFile open(DhtPath path) throws IOException {
		return DHTFile.open(path, IDFSFile.READ | IDFSFile.WRITE, table, location);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see dht.dhtfs.core.def.IFileSystem#open(dht.dhtfs.core.DhtPath, int)
	 */
	@Override
	public IFile open(DhtPath path, int mod) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete(DhtPath path) throws IOException {
		DHTFile.delete(path, table);
	}

	@Override
	public void rename(DhtPath srcPath, DhtPath dstPath) throws IOException {
		// Need optimization
		copy(srcPath, dstPath);
		DHTFile.delete(srcPath, table);
	}

	@Override
	public void copy(DhtPath srcPath, DhtPath dstPath) throws IOException {
		// TODO Auto-generated method stub
		throw new IOException("have not implemented yet");
	}

	@Override
	public void copyFromLocal(DhtPath srcPath, DhtPath dstPath) throws IOException {
		DHTFile file = DHTFile.create(dstPath, table, location);
		file.upload(srcPath.getPath());
		file.close();
		file.commit();
	}

	@Override
	public void copyToLocal(DhtPath srcPath, DhtPath dstPath) throws IOException {
		DHTFile file = DHTFile.open(srcPath, IDFSFile.READ | IDFSFile.WRITE, table, location);
		file.download(dstPath.getPath());
		file.close();
	}

	@Override
	public void mkdir(DhtPath path) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rmdir(DhtPath path, boolean recursive) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void listStatus(DhtPath path) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isDirectory(DhtPath path) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFile(DhtPath path) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean exists(DhtPath path) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
