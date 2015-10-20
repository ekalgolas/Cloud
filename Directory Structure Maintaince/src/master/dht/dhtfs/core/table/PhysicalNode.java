package master.dht.dhtfs.core.table;

import java.io.Serializable;

import master.dht.dhtfs.core.GeometryLocation;

public class PhysicalNode implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int online = 0x01;
    private static final int offline = 0x00;
    private int status;
    private String ipAddress;
    private int port;
    private GeometryLocation location;
    private String uid;

    public void dump() {
        System.out.println(ipAddress + ":" + port + " " + uid);
    }

    public int hashCode() {
        return ipAddress.hashCode() | port;
    }

    public boolean equals(Object node) {
        if (!(node instanceof PhysicalNode)) {
            return false;
        }
        PhysicalNode a = (PhysicalNode) node;
        return ipAddress.equals(a.getIpAddress()) && port == a.getPort();
    }

    public PhysicalNode(String ipAddress, int port) {
        this.setStatus(offline);
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public PhysicalNode(String ipAddress, int port, GeometryLocation location) {
        this.setStatus(offline);
        this.ipAddress = ipAddress;
        this.port = port;
        this.location = location;
    }

    public String toString() {
        return "*" + ipAddress + ":" + port + " " + location.toString() + " "
                + status + "*";
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public GeometryLocation getLocation() {
        return location;
    }

    public void setLocation(GeometryLocation location) {
        this.location = location;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public boolean isOnline() {
        return status == online;
    }

    public void online() {
        status = online;
    }

    public void offline() {
        status = offline;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

}
