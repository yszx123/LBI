package com.autonavi.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

public class TimeUtil {

	public static String getPreDayTimeStamp(String currentDay, int interval)
	{
		Calendar c = Calendar.getInstance(Locale.CHINESE);
		System.out.println("getPreDayTimeStamp" + currentDay);
		SimpleDateFormat simpleDateTimeFormat = new SimpleDateFormat("yyyyMMdd");
		try {
			c.setTime(simpleDateTimeFormat.parse(currentDay));
			c.add(Calendar.DAY_OF_MONTH, -interval);
			String y = simpleDateTimeFormat.format(c.getTime());
			return y;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
