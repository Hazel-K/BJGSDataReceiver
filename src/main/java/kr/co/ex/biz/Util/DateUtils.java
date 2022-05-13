package kr.co.ex.biz.Util;

import org.joda.time.DateTime;
import org.joda.time.DurationFieldType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateUtils {
	public static String addTime(String date, int years, int months, int days, int hours, int minutes, int seconds, String pattern) {
		DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(pattern);
		DateTime dateTime = dateTimeFormatter.parseDateTime(date);
		
		if(years != 0) {
			dateTime = dateTime.withFieldAdded(DurationFieldType.years(), years);
		}
		if(months != 0) {
			dateTime = dateTime.withFieldAdded(DurationFieldType.months(), months);
		}
		if(days != 0) {
			dateTime = dateTime.withFieldAdded(DurationFieldType.days(), days);
		}
		if(hours != 0) {
			dateTime = dateTime.withFieldAdded(DurationFieldType.hours(), hours);
		}
		if(minutes != 0) {
			dateTime = dateTime.withFieldAdded(DurationFieldType.minutes(), minutes);
		}
		if(seconds != 0) {
			dateTime = dateTime.withFieldAdded(DurationFieldType.seconds(), seconds);
		}
		
		return dateTimeFormatter.print(dateTime);
	}
}
