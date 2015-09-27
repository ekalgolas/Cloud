package dht.nio.protocol;

import java.io.Serializable;
import java.util.List;

public class ProtocolReq implements Serializable {

    private static final long serialVersionUID = 1L;

    private int rId;

    private ReqType requestType;

    public void dump() {
        System.out.println("rId: " + rId);
        System.out.println("requestType: " + requestType);
    }

    public void dumpLong(String name, List<Long> list) {
        System.out.print(name + ": ");
        if (list == null) {
            System.out.println("null");
            return;
        }
        for (Long obj : list) {
            System.out.print(obj + ", ");
        }
        System.out.println();
    }

    public void dumpInt(String name, List<Integer> list) {
        System.out.print(name + ": ");
        if (list == null) {
            System.out.println("null");
            return;
        }
        for (Integer obj : list) {
            System.out.print(obj + ", ");
        }
        System.out.println();
    }

    public void dumpStr(String name, List<String> list) {
        System.out.print(name + ": ");
        if (list == null) {
            System.out.println("null");
            return;
        }
        for (String obj : list) {
            System.out.print(obj + ", ");
        }
        System.out.println();
    }

    public ProtocolReq(ReqType requestType) {
        this.requestType = requestType;
    }

    public ProtocolReq(int rId, ReqType requestType) {
        this.setrId(rId);
        this.requestType = requestType;
    }

    public ReqType getRequestType() {
        return requestType;
    }

    public void setRequestType(ReqType requestType) {
        this.requestType = requestType;
    }

    public String toString() {
        return "rId: " + rId + " requestType: " + requestType;
    }

    public int getrId() {
        return rId;
    }

    public void setrId(int rId) {
        this.rId = rId;
    }

}
