package commons;

import java.util.Stack;

import master.Master;

import org.apache.log4j.Logger;

/**
 * Class to log performance using stop watch
 *
 * @author Ekal.Golas
 */
public class AppWatch {
	/**
	 * Watch variable
	 */
	private final Stack<Long>	watch;
	private final static Logger	LOGGER	= Logger.getLogger(Master.class);

	/**
	 * Default constructor
	 */
	public AppWatch() {
		watch = new Stack<>();
	}

	/**
	 * <pre>
	 * Starts the watch
	 * WARN: All code after this call will be monitored until stop is called
	 * </pre>
	 *
	 * @param tag
	 *            Tag this watch
	 */
	public void startWatch(final String tag) {
		watch.push(System.nanoTime());
		LOGGER.info(tag);
	}

	/**
	 * <pre>
	 * Stops the watch
	 * WARN: All code after this will not be monitored
	 * </pre>
	 *
	 * @param tag
	 *            Tag the stop event
	 * @return
	 */
	public String stopWatch(final String tag) {
		final double time = (System.nanoTime() - watch.pop()) / Math.pow(10, 6);
		return String.valueOf(time);
	}

	/**
	 * <pre>
	 * Stops the watch and logs the performance
	 * WARN: All code after this will not be monitored
	 * </pre>
	 *
	 * @param tag
	 *            Tag the stop event
	 * @return "Start Date - time taken - tag" as String
	 */
	public String stopAndLogTime(final String tag) {
		// Stop the watch
		final String time = stopWatch(tag);
		return tag + " - [" + time + " milliseconds]";
	}

	/**
	 * @return the watch
	 */
	public final Stack<Long> getWatch() {
		return watch;
	}
}