package commons;

import java.util.Date;

import org.perf4j.StopWatch;

/**
 * Class to log performance using stop watch
 *
 * @author Ekal.Golas
 */
public class AppWatch {
	/**
	 * Watch variable
	 */
	private final StopWatch	watch;

	/**
	 * Default constructor
	 */
	public AppWatch() {
		this.watch = new StopWatch();
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
		this.watch.start(tag);
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
		return this.watch.stop(tag);
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
		this.stopWatch(tag);
		final Date date = new Date(this.watch.getStartTime());
		final double time = this.watch.getElapsedTime() / 1000.0;

		return date + " - " + "[" + time + " seconds]" + " " + this.watch.getTag();
	}

	/**
	 * @return the watch
	 */
	public final StopWatch getWatch() {
		return this.watch;
	}
}