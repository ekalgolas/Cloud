package commons;

import java.io.Serializable;

/**
 * Class to represent a message sent between master and client.
 * This message can be a queried command or result of the command execution.
 */
public class Message implements Serializable {
	/**
	 * Serial version UID
	 */
	private static final long serialVersionUID = 5432433000419345082L;

	private final StringBuilder builder;

	/**
	 * Constructor
	 *
	 * @param content
	 *            Content to initialize the message with
	 */
	public Message(final String content) {
		builder = new StringBuilder(content);
	}

	/**
	 * Appends the given content to the message
	 *
	 * @param content
	 *            The content to append
	 */
	public void appendContent(final String content) {
		builder.append(content);
	}

	/**
	 * Get the content of the message
	 * 
	 * @return Content as string
	 */
	public String getContent() {
		return builder.toString();
	}
}