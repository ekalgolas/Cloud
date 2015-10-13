package commons;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * Pretty print the command output
 *
 * @author Ekal.Golas
 */
public class OutputFormatter {
	private final List<String[]>	rows	= new LinkedList<String[]>();

	/**
	 * Add a row to output
	 *
	 * @param cols
	 *            Data in columns
	 */
	public void addRow(final String... cols) {
		rows.add(cols);
	}

	/**
	 * Get the column widths for appropriate padding
	 *
	 * @return Paddings as integer array
	 */
	private int[] colWidths() {
		int cols = -1;
		for (final String[] row : rows) {
			cols = Math.max(cols, row.length);
		}

		final int[] widths = new int[cols];
		for (final String[] row : rows) {
			for (int colNum = 0; colNum < row.length; colNum++) {
				widths[colNum] = Math.max(widths[colNum], StringUtils.length(row[colNum]));
			}
		}

		return widths;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		// Get a string builder to append output
		final StringBuilder buf = new StringBuilder();
		final int[] colWidths = colWidths();

		// Write all data in rows with padding applied
		for (final String[] row : rows) {
			for (int colNum = 0; colNum < row.length; colNum++) {
				buf.append(StringUtils.rightPad(StringUtils.defaultString(row[colNum]), colWidths[colNum]));
				buf.append(' ');
			}

			buf.append('\n');
		}

		// Return the output as string
		return buf.toString();
	}
}