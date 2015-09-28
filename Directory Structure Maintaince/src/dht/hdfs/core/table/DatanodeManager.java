package dht.hdfs.core.table;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Scanner;

import dht.dhtfs.core.Configuration;
import dht.dhtfs.core.DhtPath;
import dht.dhtfs.core.GeometryLocation;
import dht.dhtfs.core.Meta;
import dht.dhtfs.core.table.PhysicalNode;

public class DatanodeManager extends Meta {
	private static final long serialVersionUID = 1L;
	private HashSet<PhysicalNode> registeredServer;

	public void initialize(Configuration config) throws IOException {
		loadDefault(config);
	}

	protected void loadDefault(Configuration config) throws IOException {
		registeredServer = new HashSet<PhysicalNode>();
		String serverFile = config.getProperty("serverFile");
		Scanner cin = null;
		try {
			cin = new Scanner(new File(serverFile));
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

		save(config.getProperty("imgFile"));
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
	
	public void dump() {
        
        for (PhysicalNode node : registeredServer) {
            System.out.println("physicalNode: " + node.toString());
        }
    }
	
	public PhysicalNode getNearestNode(DhtPath path, GeometryLocation location) {
        double minDist = Double.MAX_VALUE;
        PhysicalNode nearest = null;
        for (PhysicalNode node : registeredServer) {
            double dist = node.getLocation().distance(location);
            if (dist < minDist) {
                minDist = dist;
                nearest = node;
            }
        }
        return nearest;
    }
}
