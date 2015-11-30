package commons;

import java.io.Serializable;

/**
 * Class to represent a message sent between master and client. This message can be a queried command or result of the command execution.
 */
public class Message implements Serializable {
	/**
	 * Serial version UID
	 */
	private static final long	serialVersionUID	= 5432433000419345082L;

	private final StringBuilder	builder;
	private final StringBuilder	headerBuilder;
	private final StringBuilder	completionCode;
	private final StringBuilder	performance;

	/**
	 * Constructor
	 *
	 * @param content
	 *            Content to initialize the message with
	 */
	public Message(final String content) {
		builder = new StringBuilder(content);
		headerBuilder = new StringBuilder();
		completionCode = new StringBuilder();
		performance = new StringBuilder();
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
		completionCode = new StringBuilder();
		performance = new StringBuilder();
	}

	public Message(final String content, final String header, final String completionCode) {
		builder = new StringBuilder(content);
		headerBuilder = new StringBuilder(header);
		this.completionCode = new StringBuilder(completionCode);
		performance = new StringBuilder();
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
	public void appendHeader(final String header)
	{
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

	/**
	 * Append the given performance to the code.
	 *
	 * @param header
	 */
	public void appendPerformance(final String perf)
	{
		performance.append(perf);
	}

	/**
	 * Get the performance of the message.
	 *
	 * @return Header as String
	 */
	public String getPerformance() {
		return performance.toString();
	}

	/**
	 * Append the given text to the code.
	 *
	 * @param header
	 */
	public void appendCompletionCode(final String code)
	{
		completionCode.append(code);
	}

	/**
	 * Get the completion status for a command.
	 *
	 * @return Completion status code
	 */
	public StringBuilder getCompletionCode() {
		return completionCode;
	}

	@Override
	public String toString() {
		return "Message [builder=" + builder + ", headerBuilder=" + headerBuilder + ", completionCode=" + completionCode
				+ "]";
	}

}