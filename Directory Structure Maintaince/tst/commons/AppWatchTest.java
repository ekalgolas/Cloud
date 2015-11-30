package commons;

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

		// Start and check if the watch was created with a tag
		watch.startWatch("test");
		Assert.assertNotNull("Got null time", watch.getWatch().pop());
	}

	/**
	 * Test if we can stop the watch
	 */
	@Test
	public void testStop() {
		// Create and start the watch
		final AppWatch watch = new AppWatch();
		watch.startWatch("Test start");

		// Try to stop the watch with a tag
		final String log = watch.stopAndLogTime("Test stop");
		Assert.assertNotNull("Got null log", log);
	}
}