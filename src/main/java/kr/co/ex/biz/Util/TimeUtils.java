package kr.co.ex.biz.Util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeUtils {
	private static final Logger log = LoggerFactory.getLogger(TimeUtils.class);

	/**
	 * 강수 정시자료 조회를 위한 시간 배열입니다.</br>
	 * 전일 21시부터 금일 09시까지의 시간을 'yyyyMMddHHmm' 형식으로 표시합니다.
	 * @return String[] 시간 배열
	 */
	public static String[] get21to9Time() {
		String[] result = new String[25];
		Calendar cal = new GregorianCalendar(Locale.KOREA);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -1);
		String yesterday = new SimpleDateFormat("yyyyMMdd").format(cal.getTime()) + "0900";
		try {
			cal.setTime(sdf.parse(yesterday));
		} catch (ParseException e) {
			log.error("get21to9Time parse Error... Check String yesterday");
			return null;
		}
		
		for(int i = 0; i < result.length; i++) {
			if(i == 0) {
				result[i] = yesterday;
			} else {
				cal.add(Calendar.HOUR_OF_DAY, +1);
				result[i] = sdf.format(cal.getTime());
			}
		}
		
		return result;
	}

}