package commons.sample;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;
import commons.sample.log.Utils;
import commons.util.Log;

/**
 * Created by Yongtao on 9/19/2015.
 * <p/>
 * This is demo for how to periodically get system load info. 
 * Network static is acquired using technique in SimpleRawLogger. 
 * You can move code in main to your dedicate thread.
 * <p/>
 * Alternative choice is SIGAR@hyperic if you want more complex solution.
 */
public class SystemInfo {
	private static final Log	log	= Log.get();
	private static final long	mb	= 1024 * 1024;

	public static void main(final String args[]) throws InterruptedException, IOException {
		Utils.connectToLogServer(log);

		final OperatingSystemMXBean os = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
		final Runtime runtime = Runtime.getRuntime();

		for (;;) {
			// Please note that cpu usage may be negative,
			// because jvm takes time to calculate used cpu time over total cpu time to get ratio.
			log.i("System load: " + os.getSystemCpuLoad() + "\tProcess load: " + os.getProcessCpuLoad() + "\tFree memory: " + runtime.freeMemory() / mb +
					"MB/" + runtime.totalMemory() / mb + "MB");
			Thread.sleep(2 * 1000);
		}
	}

}
