package kr.co.ex.biz.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {
	private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);
	private static final AppProperties prop = AppProperties.getInstance();
	private static final int RESULT_OK = 200; // http res code: ok
	private static final int CONNECT_TIME_OUT = Integer.parseInt(prop.getProp("http.connecttimeout")); // 접속 대기시간
	private static final int READ_TIME_OUT = Integer.parseInt(prop.getProp("http.readtimeout")); // 읽기 대기시간
	
	public static List<String> excuteGetConnection(String url, HashMap<String, String> parameter) {
		
		List<String> result = new ArrayList<String>();
		
		int makeUrlIdx = 0;
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, String> entry : parameter.entrySet()) {
			if(makeUrlIdx == 0) {
				sb.append(url).append("?").append(entry.getKey()).append("=").append(entry.getValue());
			} else {
				sb.append("&").append(entry.getKey()).append("=").append(entry.getValue());
			}
			makeUrlIdx++;
		}
		
		final String URL = sb.toString();
		URL reqUrl = null;
		try {
			reqUrl = new URL(URL);
			HttpURLConnection http = (HttpURLConnection) reqUrl.openConnection();
			http.setRequestMethod("GET");
			http.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
			http.setConnectTimeout(CONNECT_TIME_OUT); // 10분
			http.setReadTimeout(READ_TIME_OUT); // 10분
			
			int responseCode = http.getResponseCode();
			if(responseCode == RESULT_OK) {
				BufferedReader in = new BufferedReader(new InputStreamReader(http.getInputStream(), "EUC-KR"));
				String line = "";
				
				while((line = in.readLine()) != null) {
					result.add(line);
				}
				in.close();
			}
			log.info("Requested URL:\n" + URL);
			log.info("Response Code: " + responseCode);
		} catch( MalformedURLException e ) {
			log.error("URL 구조체 생성 오류!!! URL: " + URL);
		} catch( IOException e ) {
			log.error("HTTP 커넥션 오류!!! URL: " + URL);
		}
		
		return result;
	}

}
