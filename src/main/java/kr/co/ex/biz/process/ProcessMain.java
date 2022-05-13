package kr.co.ex.biz.process;

import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kr.co.ex.biz.Util.AppProperties;
import kr.co.ex.biz.Util.NumberUtils;
import kr.co.ex.biz.WEM.WEMSchedule;

public class ProcessMain {
	private static final Logger log = LoggerFactory.getLogger(ProcessMain.class);
	private static final AppProperties prop = AppProperties.getInstance();
	private static final String METHOD1 = prop.getProp("application.runawsh");
	private static final String METHOD2 = prop.getProp("application.runsnow1");
	private static final String METHOD3 = prop.getProp("application.updateAwshCode");
	private static final String METHOD4 = prop.getProp("application.updateSnow1Code");
	private static final String UPDATEHOUR = prop.getProp("kma.code.update.hour");
	
	public static void run(String[] args) {
		String serverTime = new SimpleDateFormat("yyyyMMddHH").format(System.currentTimeMillis()) + "00";
		String execMethod = "";
		String execDate = "";
		if(args.length > 0) {
			for(int i = 0; i < args.length; i++) {
				if(i == 0) {
					execMethod = args[0];
				} else if (i == 1) {
					execDate = args[1];
				}
			}
			
			if(chkParameter(execMethod, execDate)) { return; }
			
			log.info("수동 실행함수: {}, 수동 요청기간: {}", execMethod, execDate);
			if("".equals(execDate)) {
				execDate = serverTime;
				log.info("요청기간이 존재하지 않아 서버 시간으로 전환합니다. {}", execDate);
			}
			
			if(METHOD1.equals(execMethod)) {
				WEMSchedule.getAwsh(execDate);
			} else if (METHOD2.equals(execMethod)) {
				WEMSchedule.getSnow1(execDate);
			} else if (METHOD3.equals(execMethod)) {
				WEMSchedule.updateAwshCode();
			} else if (METHOD4.equals(execMethod)) {
				WEMSchedule.updateSnow1Code();
			} 
		} else {
			log.info("정주기 정보 수집");
			WEMSchedule.getAwsh(serverTime);
			WEMSchedule.getSnow1(serverTime);
			
			String chkHour = serverTime.substring(serverTime.length() - 4, serverTime.length());
			if(UPDATEHOUR.equals(chkHour)) {
				log.info("정주기 코드 정보 갱신");
				WEMSchedule.updateAwshCode();
				WEMSchedule.updateSnow1Code();
			}
		}
	}

	private static boolean chkParameter(String execMethod, String execDate) {
		boolean result = true;
		boolean condition1 = "".equals(execMethod) || METHOD1.equals(execMethod) || METHOD2.equals(execMethod) || METHOD3.equals(execMethod) || METHOD4.equals(execMethod);
		boolean condition2 = false;
		if(execDate.length() == 0 ) {
			condition2 = true;
		} else if(execDate.length() >= 2) {
			condition2 = execDate.length() == 12 && "00".equals(execDate.substring(execDate.length()-2, execDate.length())) && NumberUtils.isNumber(execDate);
		}
		
		if (!condition1) {log.error("함수명이 적합하지 않음. application.properties에서 application.* 값 참조");}
		if (!condition2) {log.error("요청기간은 YYYYMMDDHH24MM 형식이여야 하고 MM은 00으로 고정되어야 합니다.");}
		if (condition1 && condition2) {result = false;}
 		return result;
	}
}