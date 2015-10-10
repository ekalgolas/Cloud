package commons.util;

import static commons.util.Utils.getCaller;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Created by Yongtao on 9/17/2015. 
 * This is slim wrap around java.util.logging. 
 * You might want to use log4j, etc instead but they're heavy weight. 
 * Some shorter methods are offered to save typing: severe/s, warning/w, info/i, debug/d. 
 * Also, those methods accepts throwable parameter, so you can log.w(exception e) instead of e.printStacktrace(). 
 * Also the static get() method saves the need to type class name yourself. 
 * A sample on socket logger is offered.
 */
public class Log extends Logger {
	private static Map<String, WeakReference<Log>>	cache		= new HashMap<>();
	private static final String						className	= Log.class.getName();

	protected Log(final String name, final String resourceBundleName) {
		super(name, resourceBundleName);
	}

	/**
	 * If current class name can be used, it will be passed to generate named logger. If not, an anonymous logger is returned. Loggers with same name will be
	 * cached.
	 *
	 * @return A Logger instance.
	 */
	public static Log get() {
		final String[] loggerName = getCaller(className);
		// System.out.println("Logger: "+loggerName);
		synchronized (cache) {
			final String className = loggerName[0];
			WeakReference<Log> log = cache.get(className);
			if (log == null || log.get() == null) {
				log = new WeakReference<>(new Log(className, null));
				final LogManager lm = LogManager.getLogManager();
				lm.addLogger(log.get());
			}
			return log.get();
		}
	}

	public void severe(final Throwable throwable) {
		final String[] caller = getCaller(className);
		logp(Level.SEVERE, caller[0], caller[1], "", throwable);
	}

	public synchronized void severe(final String msg, final Throwable throwable) {
		final String[] caller = getCaller(className);
		logp(Level.SEVERE, caller[0], caller[1], msg, throwable);
	}

	public synchronized void s(final String msg) {
		final String[] caller = getCaller(className);
		logp(Level.SEVERE, caller[0], caller[1], msg);
	}

	public synchronized void s(final Throwable throwable) {
		final String[] caller = getCaller(className);
		logp(Level.SEVERE, caller[0], caller[1], "", throwable);
	}

	public synchronized void s(final String msg, final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.SEVERE, caller[0], caller[1], msg, throwable);
	}

	public synchronized void warning(final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.WARNING, caller[0], caller[1], "", throwable);
	}

	public synchronized void warning(final String msg, final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.WARNING, caller[0], caller[1], msg, throwable);
	}

	public synchronized void w(final String msg) {
		final String[] caller = getCaller(className);

		logp(Level.WARNING, caller[0], caller[1], msg);
	}

	public synchronized void w(final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.WARNING, caller[0], caller[1], "", throwable);
	}

	public synchronized void w(final String msg, final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.WARNING, caller[0], caller[1], msg, throwable);
	}

	public synchronized void info(final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.INFO, caller[0], caller[1], "", throwable);
	}

	public synchronized void info(final String msg, final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.INFO, caller[0], caller[1], msg, throwable);
	}

	public synchronized void i(final String msg) {
		final String[] caller = getCaller(className);

		logp(Level.INFO, caller[0], caller[1], msg);
	}

	public synchronized void i(final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.INFO, caller[0], caller[1], "", throwable);
	}

	public synchronized void i(final String msg, final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.INFO, caller[0], caller[1], msg, throwable);
	}

	public synchronized void debug(final String msg) {
		final String[] caller = getCaller(className);

		logp(Level.FINE, caller[0], caller[1], msg);
	}

	public synchronized void debug(final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.FINE, caller[0], caller[1], "", throwable);
	}

	public synchronized void debug(final String msg, final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.FINE, caller[0], caller[1], msg, throwable);
	}

	public synchronized void d(final String msg) {
		final String[] caller = getCaller(className);

		logp(Level.FINE, caller[0], caller[1], msg);
	}

	public synchronized void d(final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.FINE, caller[0], caller[1], "", throwable);
	}

	public synchronized void d(final String msg, final Throwable throwable) {
		final String[] caller = getCaller(className);

		logp(Level.FINE, caller[0], caller[1], msg, throwable);
	}
}
