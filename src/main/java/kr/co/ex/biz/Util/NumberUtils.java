package kr.co.ex.biz.Util;

public class NumberUtils {
	public static boolean isNumber(String text) {
		boolean result = true;
		if(text == null || text.length() == 0) {
			result = false;
		} else {
			for(int i = 0; i < text.length(); i++) {
				int c = (int) text.charAt(i);
				// 숫자가 아니라면
				if(c < 48 || c > 57) {
					result = false;
				}
			}
		}
		return result;
	}
}