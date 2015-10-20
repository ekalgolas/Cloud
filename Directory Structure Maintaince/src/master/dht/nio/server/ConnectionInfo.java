package master.dht.nio.server;

import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.server.TCPServer.ReactorPool.Reactor.IOHandler.IOBuffer;

public class ConnectionInfo {
    private String ip;
    private int port;
    private IOBuffer<ProtocolReq, ProtocolResp> ioBuffer;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public IOBuffer<ProtocolReq, ProtocolResp> getIoBuffer() {
        return ioBuffer;
    }

    public void setIoBuffer(IOBuffer<ProtocolReq, ProtocolResp> ioBuffer) {
        this.ioBuffer = ioBuffer;
    }
}
