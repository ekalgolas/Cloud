package commons;

/**
 * Contains the completion status codes used in the client response.
 *
 * @author jaykay
 */
public enum CompletionStatusCode {
	SUCCESS,
	ERROR,
	NOT_FOUND,
	FILE_EXISTS,
	DIR_EXISTS,
	DIR_EXPECTED,
	UNSTABLE,
	NOT_EMPTY
}