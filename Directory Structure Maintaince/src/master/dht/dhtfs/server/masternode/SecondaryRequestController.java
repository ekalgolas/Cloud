package master.dht.dhtfs.server.masternode;

import java.io.IOException;

import master.dht.nio.client.TCPClient;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.RespType;
import master.dht.nio.protocol.proxy.TableUpdateReq;
import master.dht.nio.protocol.proxy.TableUpdateResp;
import master.dht.nio.server.ConnectionInfo;
import master.dht.nio.server.IController;

public class SecondaryRequestController implements IController {

    public SecondaryRequestController() throws IOException {
    }

    @Override
    public void initialize() throws IOException {
    }

    @Override
    public ProtocolResp process(ConnectionInfo info, ProtocolReq req) {
        ReqType reqType = req.getRequestType();
        System.out.println(reqType);
        ProtocolResp resp = null;
        switch (reqType) {
        case TABLE_UPDATE:
            resp = handleTableUpdate(info, (TableUpdateReq) req);
            break;
        default:
            ;
        }
        resp.setrId(req.getrId());
        return resp;
    }

    public ProtocolResp handleTableUpdate(ConnectionInfo info,
            TableUpdateReq req) {
        TableUpdateResp resp = new TableUpdateResp(RespType.OK);
        try {
            req.getOp().save(
                    SecondaryConfiguration.getImgDir() + "/"
                            + req.getOp().getVersion());
        } catch (IOException e) {
            resp.setResponseType(RespType.IOERROR);
            resp.setMsg("add table update log failed: " + e.getMessage());
            return resp;
        }
        return resp;
    }

    @Override
    public ProtocolReq process(TCPClient client, ProtocolResp resp) {
        // TODO Auto-generated method stub
        return null;
    }

}