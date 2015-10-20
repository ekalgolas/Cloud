package master.dht.dhtfs.core.table;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Vector;

import master.dht.dhtfs.core.GeometryLocation;
import master.dht.dhtfs.core.Saveable;
import master.dht.dhtfs.core.def.IHashFunction;
import master.dht.dhtfs.server.masternode.MasterConfiguration;
import master.dht.nio.client.TCPConnection;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.proxy.TableUpdateReq;

public class RouteTable extends Saveable {

	private static final long serialVersionUID = 1L;

	public static final int TABLE_INIT = 0x01;
	public static final int UPDATE_VNODE = 0x02;
	private static int metaReplicaNum;
	private static int blockReplicaNum;

	private static RouteTable table;

	private long version;
	private List<PhysicalNode> registeredServer;
	private List<LoadBalanceInfo> loadBalList;
	private VirtualNode vnode;

	public static RouteTable getInstance() throws IOException {
		if (table == null) {
			synchronized (RouteTable.class) {
				if (table == null) {
					try {
						table = (RouteTable) RouteTable
								.loadMeta(MasterConfiguration.getImgFile());
						table.allServerOffline();
					} catch (IOException e) {
						table = new RouteTable();
						table.initialize();
					}
					table.dump();
					TableOperation op = new TableOperation();
					op.setCmd(RouteTable.TABLE_INIT);
					op.setVersion(table.getVersion());
					op.setVpath(new ArrayList<Integer>());
					op.setNode(table.getVNode());

					// Make secondary node disable
					// table.forwardUpdateToSecondaries(op);
				}
			}
		}
		return table;
	}

	public void forwardUpdateToSecondaries(TableOperation op)
			throws IOException {
		List<PhysicalNode> nodes = MasterConfiguration.getSecondaries();
		List<TCPConnection> connections = new ArrayList<TCPConnection>();
		TableUpdateReq req = new TableUpdateReq(ReqType.TABLE_UPDATE);
		req.setOp(op);
		for (int i = 0; i < nodes.size(); ++i) {
			TCPConnection con = TCPConnection.getInstance(nodes.get(i)
					.getIpAddress(), nodes.get(i).getPort());
			connections.add(con);
			con.request(req);
		}
		boolean error = false;
		StringBuilder msg = new StringBuilder();
		for (int i = 0; i < connections.size(); ++i) {
			ProtocolResp replicaResp = connections.get(i).response();
			connections.get(i).close();
			if (replicaResp.getResponseType() != RespType.OK) {
				error = true;
				msg.append(replicaResp.getMsg() + "\n");
			}
		}
		if (error) {
			throw new IOException("table replica write failed: " + msg);
		}
	}

	public void update(TableOperation op) throws IOException {
		if (op.getCmd() == UPDATE_VNODE) {
			version = op.getVersion();
			List<Integer> vpath = op.getVpath();
			if (vpath == null || vpath.size() == 0) {
				vnode = op.getNode();
			} else {
				VirtualNode cur = vnode;
				for (int i = 0; i < vpath.size() - 1; ++i) {
					cur = cur.getVnodeByIdx(vpath.get(i));
				}
				cur.setVnodeByIdx(vpath.get(vpath.size() - 1), op.getNode());
			}
		} else {
			throw new IOException("haven't implement this table cmd: "
					+ op.getCmd());
		}
		forwardUpdateToSecondaries(op);
	}

	public VirtualNode getVNode() {
		return vnode;
	}

	private RouteTable() {
		vnode = null;
		setVersion(0);
		metaReplicaNum = MasterConfiguration.getMetaReplicaNum();
		blockReplicaNum = MasterConfiguration.getBlockReplicaNum();
	}

	private void initialize() throws IOException {
		loadDefault();
	}

	public void allServerOffline() {
		for (PhysicalNode node : registeredServer) {
			node.offline();
		}
	}

	public void setServerOffline(String uid) {
		for (PhysicalNode node : registeredServer) {
			if (node.getUid().equals(uid)) {
				node.offline();
				return;
			}
		}
	}

	public List<PhysicalNode> getOnlineServers() {
		List<PhysicalNode> online = new ArrayList<PhysicalNode>();
		for (PhysicalNode node : registeredServer) {
			if (node.isOnline()) {
				online.add(node);
			}
		}
		return online;
	}

	public void join(PhysicalNode joinNode) {
		boolean exist = false;
		for (PhysicalNode node : registeredServer) {
			if (node.equals(joinNode)) {
				node.setLocation(joinNode.getLocation());
				node.online();
				exist = true;
				break;
			}
		}
		if (!exist) {
			joinNode.online();
			registeredServer.add(joinNode);
		}
	}

	protected void loadDefault() throws IOException {
		registeredServer = new ArrayList<PhysicalNode>();
		loadBalList = new ArrayList<LoadBalanceInfo>();
		String serverFile = MasterConfiguration.getServerFile();
		Scanner cin = null;
		try {
			cin = new Scanner(new File(serverFile));
			// cin.close();
		} catch (FileNotFoundException e) {
			throw new IOException(e.getMessage(), e);
		}
		while (cin.hasNext()) {
			String domain = cin.next(), ipAddress = null;
			int port = cin.nextInt();
			double lat = cin.nextDouble();
			double lng = cin.nextDouble();
			try {
				ipAddress = InetAddress.getByName(domain).getHostAddress();
			} catch (UnknownHostException e) {
				cin.close();
				throw new IOException(e.getMessage(), e);
			}
			GeometryLocation location = new GeometryLocation(lat, lng);
			PhysicalNode node = new PhysicalNode(ipAddress, port, location);
			registeredServer.add(node);
		}
		cin.close();
		int replicaLevel = Math.min(registeredServer.size(), metaReplicaNum);
		if (registeredServer.size() < replicaLevel) {
			throw new IOException("server number is less than replica needed");
		}
		int ringLength = MasterConfiguration.getRingLength();
		List<VirtualNode> vnodes = new Vector<VirtualNode>();
		int idx = 0;
		int size = registeredServer.size();
		for (int i = 0; i < ringLength; ++i) {
			List<PhysicalNode> nodes = new Vector<PhysicalNode>();
			nodes.add(registeredServer.get(i % size));
			for (int j = 1; j < replicaLevel; ++j) {
				if (idx % size == i % size) {
					idx++;
				}
				nodes.add(registeredServer.get(idx++ % size));
			}
			vnodes.add(new VirtualNode(nodes, nodes.get(0)));
		}
		vnode = new VirtualNode(vnodes, new SimpleHash());
		save(MasterConfiguration.getImgFile());
	}

	public boolean isAvailable(PhysicalNode local, String path) {
		return getPhysicalNodes(path).contains(local);
	}

	public List<PhysicalNode> randomNodes() {
		List<PhysicalNode> nodes = new ArrayList<PhysicalNode>();
		
		//Changes to static replica number. It was blockReplicaNum before
		int n = Math.min(registeredServer.size(), 3);
		HashSet<Integer> used = new HashSet<Integer>();
		while (n-- > 0) {
			int idx = new Random().nextInt(registeredServer.size());
			while (used.contains(idx)) {
				idx = new Random().nextInt(registeredServer.size());
			}
			used.add(idx);
			nodes.add((PhysicalNode) registeredServer.toArray()[idx]);
		}
		return nodes;
	}

	// for debug
	public void dump() {
		for (PhysicalNode node : registeredServer) {
			System.out.println("physicalNode: " + node.toString());
		}
		// System.out.println("vnode: " + vnode.toString());
		// System.out.println("metaReplicaNum: " + metaReplicaNum);
		// System.out.println("blockReplicaNum: " + blockReplicaNum);
	}

	public List<PhysicalNode> getPhysicalNodes(String path) {
		return vnode.getPhysicalNodes(path);
	}

	public PhysicalNode getPrimary(String path) {
		return vnode.getPrimary(path);
	}

	public List<PhysicalNode> getAllSortedNodes(String path,
			GeometryLocation location) {
		List<PhysicalNode> sortedPhysicalNodes = new Vector<PhysicalNode>();
		List<PhysicalNode> physicalNodes = getPhysicalNodes(path);
		for (int i = 0; i < physicalNodes.size(); ++i) {
			sortedPhysicalNodes.add(physicalNodes.get(i));
		}
		for (int i = 0; i < sortedPhysicalNodes.size() - 1; ++i) {
			int minIdx = -1;
			double minDist = Double.MAX_VALUE;
			for (int j = i; j < sortedPhysicalNodes.size(); ++j) {
				double dist = sortedPhysicalNodes.get(j).getLocation()
						.distance(location);
				if (dist < minDist) {
					minIdx = j;
					minDist = dist;
				}
			}
			PhysicalNode tmp = sortedPhysicalNodes.get(minIdx);
			sortedPhysicalNodes.set(minIdx, sortedPhysicalNodes.get(i));
			sortedPhysicalNodes.set(i, tmp);
		}
		return sortedPhysicalNodes;
	}

	public PhysicalNode getNearestNode(String path, GeometryLocation location) {
		List<PhysicalNode> allNodes = getPhysicalNodes(path);
		double minDist = Double.MAX_VALUE;
		PhysicalNode nearest = null;
		for (PhysicalNode node : allNodes) {
			double dist = node.getLocation().distance(location);
			if (dist < minDist) {
				minDist = dist;
				nearest = node;
			}
		}
		return nearest;
	}

	public synchronized void save(String metaFile) throws IOException {
		super.save(metaFile);
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	class VirtualNode implements Serializable {

		private static final long serialVersionUID = 1L;

		private boolean isPhysical;

		private IHashFunction function;

		private List<VirtualNode> vnodes;

		private List<PhysicalNode> pnodes;
		private PhysicalNode primary;

		public VirtualNode(List<VirtualNode> vnodes, IHashFunction function) {
			isPhysical = false;
			this.function = function;
			this.vnodes = vnodes;
			this.pnodes = null;
			this.primary = null;
		}

		public VirtualNode(List<PhysicalNode> pnodes, PhysicalNode primary) {
			isPhysical = true;
			this.function = null;
			this.vnodes = null;
			this.pnodes = pnodes;
			this.primary = primary;
		}

		public VirtualNode getVnodeByIdx(int idx) {
			return vnodes.get(idx);
		}

		public void setVnodeByIdx(int idx, VirtualNode node) {
			vnodes.set(idx, node);
		}

		public PhysicalNode getPrimary(String path) {
			if (isPhysical) {
				return primary;
			} else {
				int vid = getVid(path);
				return vnodes.get(vid).getPrimary(path);
			}
		}

		public List<PhysicalNode> getPhysicalNodes(String path) {
			if (isPhysical) {
				return pnodes;
			} else {
				int vid = getVid(path);
				return vnodes.get(vid).getPhysicalNodes(path);
			}
		}

		protected int getVid(String path) {
			return Math.abs(function.hashValue(path)) % vnodes.size();
		}

		public String toString() {
			String str = "isPhysical: " + isPhysical;
			if (pnodes != null) {
				str += " pnodes:";
				for (PhysicalNode node : pnodes) {
					str += " " + node.toString();
				}
				str += " primary: " + primary.toString();
			}
			if (function != null) {
				str += " function: " + function.toString();
			}
			str += "\n";
			if (vnodes != null) {
				for (VirtualNode vnode : vnodes) {
					str += "\t" + vnode.toString();
				}
			}
			return str;
		}
	}

	public static class TableOperation extends Saveable {
		private static final long serialVersionUID = 1L;
		private int cmd;
		private long version;
		private List<Integer> vpath;
		private VirtualNode node;

		public int getCmd() {
			return cmd;
		}

		public void setCmd(int cmd) {
			this.cmd = cmd;
		}

		public List<Integer> getVpath() {
			return vpath;
		}

		public void setVpath(List<Integer> vpath) {
			this.vpath = vpath;
		}

		public VirtualNode getNode() {
			return node;
		}

		public void setNode(VirtualNode node) {
			this.node = node;
		}

		public long getVersion() {
			return version;
		}

		public void setVersion(long version) {
			this.version = version;
		}
	}
}
