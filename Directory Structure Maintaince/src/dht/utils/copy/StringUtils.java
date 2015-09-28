package dht.utils.copy;

public class StringUtils {
	public static final long kilo = 0x400;
	public static final long mega = 0x100000;
	public static final long giga = 0x40000000;

	public static long parseLong(String str) {
		long num = 0;
		try {
			num = Long.parseLong(str);
		} catch (NumberFormatException e) {
			num = Long.parseLong(str.substring(0, str.length() - 1));
			char u = Character.toLowerCase(str.charAt(str.length() - 1));
			switch (u) {
			case 'k':
				num *= kilo;
				break;
			case 'm':
				num *= mega;
				break;
			case 'g':
				num *= giga;
				break;
			default:
				throw new NumberFormatException("str cannot be parsed, str: "
						+ str);
			}
		}
		return num;
	}
}
