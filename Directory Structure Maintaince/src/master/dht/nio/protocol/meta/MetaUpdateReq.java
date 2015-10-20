package master.dht.nio.protocol.meta;

import master.dht.dhtfs.server.datanode.FileMeta;
import master.dht.nio.protocol.ProtocolReq;
import master.dht.nio.protocol.ReqType;

public class MetaUpdateReq extends ProtocolReq {

    private static final long serialVersionUID = 1L;

    private FileMeta fileMeta;
    private boolean create;
    private boolean delete;

    public void dump() {
        System.out.println("***********BEGIN***********");
        super.dump();
        System.out.println("fileMeta: " + fileMeta.toString());
        System.out.println("***********END***********");
        create = false;
    }

    public MetaUpdateReq(ReqType requestType) {
        super(requestType);
    }

    public MetaUpdateReq(int rId, ReqType requestType) {
        super(rId, requestType);
    }

    public FileMeta getFileMeta() {
        return fileMeta;
    }

    public void setFileMeta(FileMeta fileMeta) {
        this.fileMeta = fileMeta;
    }

    public boolean isCreate() {
		return create;
	}

	public void setCreate(boolean create) {
		this.create = create;
	}

	public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }

}
