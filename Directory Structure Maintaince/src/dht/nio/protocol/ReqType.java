package dht.nio.protocol;

public enum ReqType {
	SERVER_REPORT,

	JOIN, TABLE,

	CREATE_FILE, OPEN_FILE, DELETE_FILE, COMMIT_FILE,

	READ_FILE, WRITE_FILE,

}
