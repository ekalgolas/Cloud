package dht.dhtfs.core.table;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
import java.util.Vector;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.Meta;
import dht.dhtfs.core.def.IHashFunction;

public class RouteTable extends Meta {

	private static final long serialVersionUID = 1L;
	private HashSet<PhysicalNode> registeredServer;

	private Vector<VirtualNode> vnodes;

	private IHashFunction function;

	public RouteTable() {
		vnodes = new Vector<VirtualNode>();
		function = null;
	}

	public void initialize(Configuration config) throws IOException {
		loadDefault(config);
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

	protected void loadDefault(Configuration config) throws IOException {
		registeredServer = new HashSet<PhysicalNode>();
		function = new SimpleHash();
		function.setDescription("simple hash");
		String serverFile = config.getProperty("serverFile");
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
			double x = cin.nextDouble();
			double y = cin.nextDouble();
			try {
				ipAddress = InetAddress.getByName(domain).getHostAddress();
			} catch (UnknownHostException e) {
				cin.close();
				throw new IOException(e.getMessage(), e);
			}
			GeometryLocation location = new GeometryLocation(x, y);
			PhysicalNode node = new PhysicalNode(ipAddress, port, location);
			registeredServer.add(node);
		}
		cin.close();
		int replicaLevel = Integer.parseInt(config.getProperty("replicaLevel"));
		if (registeredServer.size() < replicaLevel) {
			throw new IOException("server number is less than replica needed");
		}
		int ringLength = Integer.parseInt(config.getProperty("ringLength"));
		int seed = Integer.parseInt(config.getProperty("seed"));
		Random ran = new Random(seed);
		for (int i = 0; i < ringLength; ++i) {
			HashSet<Integer> used = new HashSet<Integer>();
			int primaryIdx = ran.nextInt(registeredServer.size());
			used.add(primaryIdx);
			PhysicalNode primary = (PhysicalNode) registeredServer.toArray()[primaryIdx];
			Vector<PhysicalNode> secondaries = new Vector<PhysicalNode>();
			for (int j = 1; j < replicaLevel; ++j) {
				while (true) {
					int secondaryIdx = ran.nextInt(registeredServer.size());
					if (!used.contains(secondaryIdx)) {
						secondaries.add((PhysicalNode) registeredServer.toArray()[secondaryIdx]);
						used.add(secondaryIdx);
						break;
					}
				}
			}
			VirtualNode vnode = new VirtualNode(primary, secondaries);
			vnodes.add(vnode);
		}
		save(config.getProperty("imgFile"));
	}

	public boolean isAvailable(PhysicalNode local, DhtPath path) {
		return getAllNodes(path).contains(local);
	}

	// public RouteTable load(String imgFile) throws IOException {
	// FileInputStream fis = new FileInputStream(imgFile);
	// ObjectInputStream ois = new ObjectInputStream(fis);
	// RouteTable table = null;
	// // Vector<VirtualNode> obj1 = null;
	// // IHashFunction obj2 = null;
	// try {
	// table = (RouteTable) ois.readObject();
	// // obj1 = (Vector<VirtualNode>) ois.readObject();
	// // obj2 = (IHashFunction) ois.readObject();
	// } catch (ClassNotFoundException e) {
	// throw new IOException(e.getMessage(), e);
	// } finally {
	// ois.close();
	// }
	// if (table != null) {
	// this.registeredServer = table.registeredServer;
	// this.vnodes = table.vnodes;
	// this.function = table.function;
	// // if (obj1 != null && obj2 != null) {
	// // vnodes = obj1;
	// // function = obj2;
	// } else {
	// throw new IOException("load imgFile failed");
	// }
	// }

	// public synchronized void save(String imgFile) throws IOException {
	// FileOutputStream fos = new FileOutputStream(imgFile);
	// ObjectOutputStream oos = new ObjectOutputStream(fos);
	// oos.writeObject(this);
	// oos.close();
	// }

	// for debug
	public void dump() {
		for (int i = 0; i < vnodes.size(); ++i) {
			VirtualNode vnode = vnodes.get(i);
			System.out.println("vnode " + i + ": " + vnode.toString());
		}
		for (PhysicalNode node : registeredServer) {
			System.out.println("physicalNode: " + node.toString());
		}
		System.out.println(function.toString());
	}

	public PhysicalNode getPrimaryNode(DhtPath path) {
		int vid = getVid(path);
		return vnodes.get(vid).getPrimaryNode(path);
	}

	public Vector<PhysicalNode> getSecondaryNodes(DhtPath path) {
		int vid = getVid(path);
		return vnodes.get(vid).getSecondaryNodes(path);
	}

	public Vector<PhysicalNode> getAllNodes(DhtPath path) {
		int vid = getVid(path);
		return vnodes.get(vid).getAllNodes(path);
	}

	public Vector<PhysicalNode> getAllSortedNodes(DhtPath path, GeometryLocation location) {
		Vector<PhysicalNode> sortedPhysicalNodes = new Vector<PhysicalNode>();
		Vector<PhysicalNode> physicalNodes = getAllNodes(path);
		for (int i = 0; i < physicalNodes.size(); ++i) {
			sortedPhysicalNodes.add(physicalNodes.get(i));
		}
		for (int i = 0; i < sortedPhysicalNodes.size() - 1; ++i) {
			int minIdx = -1;
			double minDist = Double.MAX_VALUE;
			for (int j = i; j < sortedPhysicalNodes.size(); ++j) {
				double dist = sortedPhysicalNodes.get(j).getLocation().distance(location);
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

	public PhysicalNode getNearestNode(DhtPath path, GeometryLocation location) {
		Vector<PhysicalNode> allNodes = getAllNodes(path);
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

	protected int getVid(DhtPath path) {
		// System.out.println(path.getName());
		return Math.abs(function.hashValue(path.getName())) % vnodes.size();
	}

	public Vector<Integer> getVTrace(DhtPath path) {
		Vector<Integer> trace = new Vector<Integer>();
		int vid = getVid(path);
		trace.add(vid);
		VirtualNode node = vnodes.get(vid);
		while (!node.isLeaf()) {
			vid = node.getVid(path);
			trace.add(vid);
			node = node.getVnodes().get(vid);
		}
		return trace;
	}

	public synchronized void save(String metaFile) throws IOException {
		super.save(metaFile);
	}

	class VirtualNode implements Serializable {

		private static final long serialVersionUID = 1L;

		private boolean isLeaf;

		private IHashFunction function;

		private Vector<VirtualNode> vnodes;

		private PhysicalNode primary;

		private Vector<PhysicalNode> secondaries;

		public PhysicalNode getPrimaryNode(DhtPath path) {
			if (isLeaf) {
				return getPrimary();
			} else {
				int vid = getVid(path);
				return vnodes.get(vid).getPrimaryNode(path);
			}
		}

		public Vector<PhysicalNode> getSecondaryNodes(DhtPath path) {
			if (isLeaf) {
				return getSecondaries();
			} else {
				int vid = getVid(path);
				return vnodes.get(vid).getSecondaryNodes(path);
			}
		}

		public Vector<PhysicalNode> getAllNodes(DhtPath path) {
			if (isLeaf) {
				Vector<PhysicalNode> nodes = new Vector<PhysicalNode>();
				nodes.add(getPrimary());
				nodes.addAll(getSecondaries());
				return nodes;
			} else {
				int vid = getVid(path);
				return vnodes.get(vid).getAllNodes(path);
			}
		}

		public String toString() {
			String str = "isLeaf: " + isLeaf;
			if (function != null) {
				str += " function: " + function.toString();
			}
			if (vnodes != null) {
				str += " vnodes:";
				for (VirtualNode vnode : vnodes) {
					str += " " + vnode.toString();
				}
			}
			if (primary != null) {
				str += " primary: " + primary.toString();
			}
			if (secondaries != null) {
				str += " secondaries:";
				for (PhysicalNode node : secondaries) {
					str += " " + node.toString();
				}
			}
			return str;
		}

		public VirtualNode(Vector<VirtualNode> vnodes, IHashFunction function) {
			isLeaf = false;
			this.vnodes = vnodes;
			this.function = function;
			this.primary = null;
			this.secondaries = null;
		}

		public VirtualNode(PhysicalNode primary, Vector<PhysicalNode> secondaries) {
			isLeaf = true;
			this.vnodes = null;
			this.function = null;
			this.primary = primary;
			this.secondaries = secondaries;
		}

		public boolean isLeaf() {
			return isLeaf;
		}

		public void setLeaf(boolean isLeaf) {
			this.isLeaf = isLeaf;
		}

		public PhysicalNode getPrimary() {
			return primary;
		}

		public void setPrimary(PhysicalNode primary) {
			this.primary = primary;
		}

		public Vector<PhysicalNode> getSecondaries() {
			return secondaries;
		}

		public void setSecondaries(Vector<PhysicalNode> secondaries) {
			this.secondaries = secondaries;
		}

		public Vector<VirtualNode> getVnodes() {
			return vnodes;
		}

		public void setVnodes(Vector<VirtualNode> vnodes) {
			this.vnodes = vnodes;
		}

		protected int getVid(DhtPath path) {
			return Math.abs(function.hashValue(path.getName())) % vnodes.size();
		}

	}
}
