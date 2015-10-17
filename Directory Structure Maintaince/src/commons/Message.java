package commons;

import java.io.Serializable;

/**
 * Class to represent a message sent between master and client. This message can
 * be a queried command or result of the command execution.
 */
public class Message implements Serializable {
	/**
	 * Serial version UID
	 */
	private static final long serialVersionUID = 5432433000419345082L;

	private final StringBuilder builder;
	private final StringBuilder headerBuilder;

	/**
	 * Constructor
	 *
	 * @param content
	 *            Content to initialize the message with
	 */
	public Message(final String content) {
		builder = new StringBuilder(content);
		headerBuilder = new StringBuilder();
	}

	/**
	 * Constructor to initialise both header and content.
	 * 
	 * @param content
	 * @param header
	 */
	public Message(final String content, final String header) {
		builder = new StringBuilder(content);
		headerBuilder = new StringBuilder(header);
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
	 * Append the given text to the header.
	 * 
	 * @param header
	 */
	public void appendHeader(final String header) {
		headerBuilder.append(header);
	}

	/**
	 * Get the content of the message
	 * 
	 * @return Content as string
	 */
	public String getContent() {
		return builder.toString();
	}

	/**
	 * Get the Header of the message.
	 * 
	 * @return Header as String
	 */
	public String getHeader() {
		return headerBuilder.toString();
	}

}