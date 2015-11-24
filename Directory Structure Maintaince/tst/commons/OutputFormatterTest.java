package commons;

import static org.junit.Assert.assertEquals;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

/**
 * Class to test formatting of output
 *
 * @author Ekal.Golas
 */
public class OutputFormatterTest {
	/**
	 * <pre>
	 * Check if we can add multiple rows for output
	 * Test method for {@link commons.OutputFormatter#toString()}.
	 * </pre>
	 */
	@Test
	public void testToString() {
		// Create object for formatter
		final OutputFormatter formatter = new OutputFormatter();

		// Add some test data
		formatter.addRow("No.", "Name");
		formatter.addRow("1", "Test one");
		formatter.addRow("10", "Test ten");
		formatter.addRow("100", "Test Hundred", "Added one more column");

		// Get the formatted output
		final String output = formatter.toString();

		// Get rows
		final String[] rows = output.split("\n");
		String[] previousCol = rows[0].split("\t");
		for (int i = 1; i < rows.length; i++) {
			final String[] cols = rows[i].split("\t");

			// Do for each column
			for (int j = 1; j < cols.length; j++) {
				// Compare indentation with column in the previous row
				if (previousCol.length > j) {
					assertEquals("Indentation did not match", rows[i - 1].indexOf(previousCol[j]), rows[i].indexOf(cols[j]));
				}
			}

			previousCol = ArrayUtils.clone(cols);
		}
	}
}