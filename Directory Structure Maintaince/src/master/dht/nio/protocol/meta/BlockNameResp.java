package master.dht.nio.protocol.meta;

import java.util.List;

import master.dht.dhtfs.core.table.PhysicalNode;
import master.dht.nio.protocol.ProtocolResp;
import master.dht.nio.protocol.RespType;

public class BlockNameResp extends ProtocolResp {

	private static final long serialVersionUID = 1L;

	private List<Integer> newBlkSizes;
	private List<String> newBlkNames;
	private List<List<PhysicalNode>> newBlkServers;

	public void dump() {
		System.out.println("***********BEGIN***********");
		super.dump();

		System.out.println("newBlkSizes:");
		for (int i = 0; i < newBlkSizes.size(); i++) {
			System.out.print(newBlkSizes.get(i) + " ");
		}
		System.out.println();

		System.out.println("newBlkNames:");
		for (int i = 0; i < newBlkNames.size(); i++) {
			System.out.print(newBlkNames.get(i) + " ");
		}
		System.out.println();

		System.out.println("newBlkServers:");
		for (int i = 0; i < newBlkServers.size(); i++) {
			for (int j = 0; j < newBlkServers.get(i).size(); j++) {
				System.out.print(newBlkServers.get(i).get(j).getPort() + " ");
			}
			System.out.println();
		}
		System.out.println("***********END***********");
	}

	public BlockNameResp(RespType responseType) {
		super(responseType);
	}

	public BlockNameResp(int rId, RespType responseType) {
		super(rId, responseType);
	}

	public List<String> getNewBlkNames() {
		return newBlkNames;
	}

	public void setNewBlkNames(List<String> newBlkNames) {
		this.newBlkNames = newBlkNames;
	}

	public List<Integer> getNewBlkSizes() {
		return newBlkSizes;
	}

	public void setNewBlkSizes(List<Integer> newBlkSizes) {
		this.newBlkSizes = newBlkSizes;
	}

	public List<List<PhysicalNode>> getNewBlkServers() {
		return newBlkServers;
	}

	public void setNewBlkServers(List<List<PhysicalNode>> newBlkServers) {
		this.newBlkServers = newBlkServers;
	}

}
