package task.task_procedure;

public class CipherHelper {
	public static String fillChar(String key,int minLen) {
		if (key == null || key.length()<8) {
			return Crypto.DEFAULT_SECRET_KEY;
		}
		int leng = key.length();

		if (leng < minLen) {
			for (int i = leng; i < minLen; i++) {
				key += "m";
			}
			return key;
		}
		return key;
	}
}
