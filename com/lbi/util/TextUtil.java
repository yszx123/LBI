package com.lbi.util;

import java.util.StringTokenizer;

public class TextUtil {

	
	public static String[] split(String input, String delim, int size) {
		String str[] = null;
		try {
			if (delim == null) {
				delim = ",";
			}
			StringTokenizer stk = new StringTokenizer(input, delim, true);
//			int n = TextUtil.countStr(input, delim);
			str = new String[size];
			int index = -1;
			while (stk.hasMoreTokens()) {
				String e = stk.nextToken();
				if (e.equalsIgnoreCase(delim)) {
					index++;
					continue;
				}
				if(index + 1 < str.length)str[index + 1] = e;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return str;
	}
}
