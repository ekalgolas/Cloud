package dht.nio.protocol;

import java.io.Serializable;
import java.util.List;

public class ProtocolResp implements Serializable {

    private static final long serialVersionUID = 1L;
    private int rId;
    private RespType responseType;
    private String msg;

    public void dump() {
        System.out.println("rId: " + rId);
        System.out.println("responseType: " + responseType);
        System.out.println("msg: " + msg);
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

    public ProtocolResp(RespType responseType) {
        this.responseType = responseType;
    }

    public ProtocolResp(int rId, RespType responseType) {
        this.setrId(rId);
        this.responseType = responseType;
    }

    public RespType getResponseType() {
        return responseType;
    }

    public void setResponseType(RespType responseType) {
        this.responseType = responseType;
    }

    public int getrId() {
        return rId;
    }

    public void setrId(int rId) {
        this.rId = rId;
    }

    public String toString() {
        return "rId: " + rId + " responseType: " + responseType;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

}
