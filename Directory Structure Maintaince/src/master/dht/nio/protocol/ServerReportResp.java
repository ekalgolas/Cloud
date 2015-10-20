package master.dht.nio.protocol;
public class ServerReportResp extends ProtocolResp {

	private static final long serialVersionUID = 1L;

	private int ioReactorNum;
	private int[] reactorConnectionNum;
	private long[] reactorReqNum;
	private long[] reactorRespNum;
	private long[] reactorByteReceived;
	private long[] reactorByteSent;

	public ServerReportResp(RespType responseType) {
		super(responseType);
	}

	public ServerReportResp(int rId, RespType responseType) {
		super(rId, responseType);
	}

	public int getIoReactorNum() {
		return ioReactorNum;
	}

	public void setIoReactorNum(int ioReactorNum) {
		this.ioReactorNum = ioReactorNum;
	}

	public int[] getReactorConnectionNum() {
		return reactorConnectionNum;
	}

	public void setReactorConnectionNum(int[] reactorConnectionNum) {
		this.reactorConnectionNum = reactorConnectionNum;
	}

	public long[] getReactorReqNum() {
		return reactorReqNum;
	}

	public void setReactorReqNum(long[] reactorReqNum) {
		this.reactorReqNum = reactorReqNum;
	}

	public long[] getReactorRespNum() {
		return reactorRespNum;
	}

	public void setReactorRespNum(long[] reactorRespNum) {
		this.reactorRespNum = reactorRespNum;
	}

	public long[] getReactorByteReceived() {
		return reactorByteReceived;
	}

	public void setReactorByteReceived(long[] reactorByteReceived) {
		this.reactorByteReceived = reactorByteReceived;
	}

	public long[] getReactorByteSent() {
		return reactorByteSent;
	}

	public void setReactorByteSent(long[] reactorByteSent) {
		this.reactorByteSent = reactorByteSent;
	}

	public String toString() {
		int totalConnection = 0;
		String s = super.toString();
		for (int i = 0; i < ioReactorNum; ++i) {
			totalConnection += reactorConnectionNum[i];
		}
		s += " ioReactorNum: " + ioReactorNum + " totalConnection: "
				+ totalConnection + "\n";
		for (int i = 0; i < ioReactorNum; ++i) {
			s += "reactor[" + i + "]: " + " connection: "
					+ reactorConnectionNum[i] + " req: " + reactorReqNum[i]
					+ " resp: " + reactorRespNum[i] + " received: "
					+ reactorByteReceived[i] + " sent: " + reactorByteSent[i]
					+ "\n";
		}
		return s;
	}
}
