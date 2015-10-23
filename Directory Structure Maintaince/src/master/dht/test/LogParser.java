package master.dht.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import master.dht.dhtfs.server.datanode.DataServerConfiguration;
import master.dht.nio.protocol.ReqType;
import master.dht.nio.protocol.loadbalancing.LoadBalReq;

public class LogParser {
	private static int logThreasoldMinutes = 180;

	public static void main(String[] args) {
		String csvFile = "./example.log";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\\s+";
		String[] split;
		String[] time;

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		Date now = cal.getTime();

		LoadBalReq loadBalReq = new LoadBalReq(ReqType.LOAD_BAL);
		File root = new File("/");
		// File metaDir = new File(DataServerConfiguration.getMetaDir());
		int totalMetaCount = 0;
		// getTotalNoOfFile(metaDir, totalMetaCount);
		int totalReadReq = 0;
		int totalWriteReq = 0;
		ArrayList<Long> readBlockList = new ArrayList<>();
		ArrayList<Long> writeBlockList = new ArrayList<>();
		int diskUtilization = (int) (root.getFreeSpace() / root.getTotalSpace());
		int activeTCPConnections = 0;
		ArrayList<Long> readMetaList = new ArrayList<>();
		ArrayList<Long> writeMetaList = new ArrayList<>();
		long length;

		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				split = line.split(cvsSplitBy);

				time = split[1].split(",");
				StringBuilder sb = new StringBuilder(split[0]);
				sb.append(" ").append(time[0]);
				Date date = sdf.parse(sb.toString());
				long diff = date.getTime() - now.getTime();
				long diffMinutes = Math.abs(diff / (60 * 1000));

				if (diffMinutes > logThreasoldMinutes) {
					break;
				}
			}

			do {
				split = line.split(cvsSplitBy);

				String info = split[4];
				String type = split[6];

				if (info.equals("[meta]")) {
					length = Long.parseLong(split[10]);
					switch (type) {
					case "Read":
						readMetaList.add(length);
						break;
					case "Write":
						writeMetaList.add(length);
						break;
					default:
						break;
					}
				} else {
					String blockName;
					long blockVersion;
					switch (type) {
					case "Read":
						blockName = split[8];
						blockVersion = Long.parseLong(split[10]);
						length = Long.parseLong(split[14]);
						totalReadReq++;
						readBlockList.add(length);
						break;
					case "Write":
						length = Long.parseLong(split[14]);
						totalWriteReq++;
						writeBlockList.add(length);
						break;
					case "Connect":
						activeTCPConnections++;
						break;
					case "Close":
						activeTCPConnections--;
						break;
					default:
						break;
					}
				}

			} while ((line = br.readLine()) != null);

			loadBalReq.setReadReqPerMin(totalReadReq / logThreasoldMinutes);
			loadBalReq.setWriteReqPerMin(totalWriteReq / logThreasoldMinutes);
			loadBalReq.setReadBlock(readBlockList);
			loadBalReq.setWriteBlock(writeBlockList);
			loadBalReq.setDiskUtilization(diskUtilization);
			loadBalReq.setActiveTCPConnections(activeTCPConnections);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		System.out.println("Done");

	}

	private static void getTotalNoOfFile(File metaDir, int count) {
		File[] files = metaDir.listFiles();

		for (File file : files) {
			if (file.isDirectory()) {
				getTotalNoOfFile(file, count);
			} else {
				count++;
			}
		}
	}
}
