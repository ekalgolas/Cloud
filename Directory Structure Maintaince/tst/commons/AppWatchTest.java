package commons;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

/**
 * Class to test the performance logging ability
 *
 * @author Ekal.Golas
 */
public class AppWatchTest {
	/**
	 * Test Creation of watch
	 */
	@Test
	public void testCreate() {
		try {
			new AppWatch();
		} catch (final Exception e) {
			Assert.fail("Cannot create watch");
		}
	}

	/**
	 * Test if we can start the watch
	 */
	@Test
	public void startTest() {
		// Create the watch
		final AppWatch watch = new AppWatch();

		// Start and check if the watch was created with null tag
		watch.startWatch(null);
		Assert.assertNull("Tag incorrect", watch.getWatch().getTag());

		// Start and check if the watch was created with a tag
		watch.startWatch("test");
		Assert.assertEquals("Tag incorrect", "test", watch.getWatch().getTag());
	}

	/**
	 * Test if we can stop the watch
	 */
	@Test
	public void testStop() {
		// Create and start the watch
		final AppWatch watch = new AppWatch();
		watch.startWatch("Test start");

		// Try to stop the watch with null tag
		final Date date = new Date(watch.getWatch().getStartTime());
		String log = watch.stopAndLogTime(null);
		double elapsed = watch.getWatch().getElapsedTime() / 1000.0;
		Assert.assertEquals("Log incorrect", date + " - [" + elapsed + " seconds] null", log);

		// Try to stop the watch with a tag
		log = watch.stopAndLogTime("Test stop");
		elapsed = watch.getWatch().getElapsedTime() / 1000.0;
		Assert.assertEquals("Log incorrect", date + " - [" + elapsed + " seconds] Test stop", log);
	}
}