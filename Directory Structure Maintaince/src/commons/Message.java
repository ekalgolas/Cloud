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

	private String content;
	private StringBuilder builder;
	
	public Message(String content) {
		builder = new StringBuilder(content);
	}

	public void appendContent(String content) {
		builder.append(content);
	}
	
	public String getContent() {
		content = builder.toString();
		return content;
	}
}
