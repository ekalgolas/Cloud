package commons;

import org.junit.Assert;
import org.junit.Test;

/**
 * Class to master.dht.test the serializable message
 *
 * @author Ekal.Golas
 */
public class MessageTest {
	/**
	 * To master.dht.test if we can serialize a string to a message object
	 */
	@Test
	public void createMessageTest() {
		// Get a master.dht.test string
		final String testString = "This is a master.dht.test string";

		// Try to serialize as a message
		final Message message = new Message(testString);
		Assert.assertNotNull("Serialization unsuccessful: Null object returned", message);
	}

	/**
	 * To master.dht.test if we can get content back from a serialized object
	 */
	@Test
	public void deserializeTest() {
		// Get a master.dht.test string
		final String testString = "This is a master.dht.test string";

		// Try to serialize as a message
		final Message message = new Message(testString);

		// Get the content of the message
		final String content = message.getContent();
		Assert.assertEquals("Deserialized content and actual content are different", testString, content);
	}

	/**
	 * To master.dht.test if we can append content to serialized object
	 */
	@Test
	public void appendTest() {
		// Get master.dht.test strings
		final String testString = "This is a master.dht.test string";
		final String testString2 = " and so is this too";

		// Try to serialize as a message
		final Message message = new Message(testString);

		// Append content
		message.appendContent(testString2);

		// Get the content of the message
		final String content = message.getContent();
		Assert.assertEquals("Deserialized content after append and actual content are different", testString + testString2, content);
	}
}