package client;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import commons.AppConfig;
import commons.Globals;
import commons.Message;

/**
 * Class to write a CSV file
 *
 * @author Ekal.Golas
 */
public class CSVFileWriter {
	private static String	file	= AppConfig.getValue("client.resultsFile");

	/**
	 * Appends the command performance results to a CSV file
	 *
	 * @param message
	 *            Reply result of the command
	 * @param command
	 *            Command that was executed
	 * @throws IOException
	 */
	public static void writeToFile(final Message message,
			final String command)
			throws IOException {
		// Find and match the time taken
		final Pattern p = Pattern.compile("\\[([^\\]]*)\\]");
		final Matcher matcher = p.matcher(message.getPerformance());
		String time = null;
		if (matcher.find()) {
			final String[] match = matcher.group(1).split(" ");
			time = match[0];
		}

		// Get level, completion code and command type
		final String[] splits = command.split(" ");
		final String level = splits[1].split("/").length + "";
		final String result = message.getCompletionCode().toString();

		// Get type of file system
		final String port = AppConfig.getValue("client.masterPort");
		String type = null;
		if (port.equals(AppConfig.getValue(Globals.GFS_SERVER_PORT))) {
			type = "GFS";
		} else if (port.equals(AppConfig.getValue(Globals.NFS_SERVER_PORT))) {
			type = "NFS";
		} else {
			type = "CEPH";
		}

		// Write result to CSV
		final String line = splits[0] + "," + type + "," + level + "," + time + "," + result + "\n";
		FileUtils.write(new File(file), line, true);
	}
}