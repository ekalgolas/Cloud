package master.dht.dhtfs.server.masternode;

import java.io.IOException;

import master.dht.dhtfs.core.GeometryLocation;
import master.dht.dhtfs.core.def.IIDAssigner;
import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.dhtfs.core.table.RouteTable;
import master.dht.nio.client.TCPClient;
import master.dht.nio.protocol.ConnectionErrorResp;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.proxy.HeartBeatResp;
import master.dht.nio.protocol.proxy.JoinReq;
import master.dht.nio.protocol.proxy.JoinResp;
import master.dht.nio.protocol.proxy.TableReq;
import master.dht.nio.protocol.proxy.TableResp;
import master.dht.nio.server.ConnectionInfo;
import master.dht.nio.server.IController;

public class MasterRequestController implements IController {

    protected RouteTable table;
    private IIDAssigner serverIdAssigner;

    public MasterRequestController() throws IOException {
        serverIdAssigner = new ServerIdAssigner(
                MasterConfiguration.getIdSeqFile());
    }

    @Override
    public void initialize() throws IOException {
        table = RouteTable.getInstance();
    }

    @Override
    public ProtocolResp process(ConnectionInfo info, ProtocolReq req) {
        ReqType reqType = req.getRequestType();
        System.out.println(reqType);
        ProtocolResp resp = null;
        switch (reqType) {
        case JOIN:
            resp = handleJoin(info, (JoinReq) req);
            break;
        case TABLE:
            resp = handleTableReq(info, (TableReq) req);
            break;
        default:
            ;
        }
        resp.setrId(req.getrId());
        return resp;
    }

    public ProtocolResp handleJoin(ConnectionInfo info, JoinReq req) {
        JoinResp resp = new JoinResp(RespType.OK);
        PhysicalNode joinNode = new PhysicalNode(info.getIp(), req.getPort());
        GeometryLocation location = req.getLocation();
        joinNode.setLocation(location);
        String uid = req.getUid();
        if (uid == null) {
            try {
                uid = serverIdAssigner.generateUID();
            } catch (IOException e) {
                resp.setResponseType(RespType.IOERROR);
                resp.setMsg("Generate UID failed: " + e.getMessage());
                return resp;
            }
        }
        joinNode.setUid(uid);
        table.join(joinNode);
        resp.setTable(table);
        resp.setLocal(joinNode);
        return resp;
    }

    public ProtocolResp handleTableReq(ConnectionInfo info, TableReq req) {
        TableResp resp = new TableResp(RespType.OK);
        resp.setTable(table);
        return resp;
    }

    @Override
    public ProtocolReq process(TCPClient client, ProtocolResp resp) {
        RespType respType = resp.getResponseType();
        System.out.println(respType);
        ProtocolReq req = null;
        switch (respType) {
        case CONNECTION_ERROR:
            resp = handleConnectionErrorResp(client, (ConnectionErrorResp) resp);
            break;
        case OK:
            resp = handleHeartBeatResp(client, (HeartBeatResp) resp);
            break;
        default:
            ;
        }
        return req;
    }

    ProtocolResp handleConnectionErrorResp(TCPClient client,
            ConnectionErrorResp resp) {
        String uid = resp.getUid();
        table.setServerOffline(uid);
        try {
            client.deregister(uid);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    ProtocolResp handleHeartBeatResp(TCPClient client, HeartBeatResp resp) {
        String uid = resp.getUid();
        table.setServerOffline(uid);
        try {
            client.deregister(uid);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
