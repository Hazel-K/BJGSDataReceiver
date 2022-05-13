package kr.co.ex.biz.WEM;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kr.co.ex.biz.Util.AppProperties;
import kr.co.ex.biz.Util.HttpUtils;

public class WEMSchedule {
	private static final Logger log = LoggerFactory.getLogger(WEMSchedule.class);
	private static final AppProperties prop = AppProperties.getInstance();
	private static final String APIKEY = prop.getProp("kma.apikey");
	private static final String URL1 = prop.getProp("kma.awsh.url");
	private static final String URL2 = prop.getProp("kma.snow1.url");
	private static final String URL3 = prop.getProp("kma.awsh.code.url");
	private static final String URL4 = prop.getProp("kma.snow1.code.url");
	
	private static final WEMDAO wemDao = new WEMDAO();
	
	public static void getAwsh(String serverTime) {
		log.info("K11.시간통계_정시자료 정시데이터 수집...");
		
		HashMap<String, String> parameter = new HashMap<String, String>();
		// 강수 정시자료
		parameter.put("authKey", APIKEY);
		parameter.put("tm", serverTime);
		
		List<String> ret = HttpUtils.excuteGetConnection(URL1, parameter);
		List<Map<String, Object>> resultList = new ArrayList<Map<String,Object>>();
		String[] colNmArr = prop.getProp("kma.awsh.arr").split(",");
//		String sample = "200911121500    12   13.6   82.6    5.3    0.0    0.0   46.8 1023.2 1028.7";
		
		for(String item : ret) {
			if(item.contains("#")) { continue; }
			else {
				Map<String, Object> parsedMap = new HashMap<String, Object>();
				parsedMap = initializeMap(parsedMap, colNmArr);
				int colIdx = 0;
				String[] fieldValueArr = item.split(" ");
				for(String fieldValue : fieldValueArr) {
					if(fieldValue.length() == 0) { continue; }
					else {
						if(colIdx == 0) {
							parsedMap.put(colNmArr[colIdx++], fieldValue);
							parsedMap.put(colNmArr[colIdx++], fieldValue.substring(0, 8));
							parsedMap.put(colNmArr[colIdx++], fieldValue.substring(8, 12));
						} else {
							parsedMap.put(colNmArr[colIdx++], fieldValue);
						}
					}
				}
				resultList.add(parsedMap);
			}
		}
		
		log.info("size: " + resultList.size());
		int sqlIdx = 0;
		int resultCnt = 0;
		for(Map<String, Object> item : resultList) {
			log.info("{}번째 DATA 삽입중...", ++sqlIdx);
			int setCnt = wemDao.mergeAwshData(item);
			resultCnt += setCnt;
		}
		
		Map<String, Object> logMap = new HashMap<String, Object>();
		logMap.put("IF_ID", prop.getProp("esi.interface.awsh"));
		logMap.put("DATA_CNT", resultCnt);
		logMap.put("CRE_DTTM", new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis()));
		if(resultCnt > 0) {
			logMap.put("TRST_STAT", "C");
			logMap.put("ERR_MSG", "");
		} else {
			logMap.put("TRST_STAT", "E");
			logMap.put("ERR_MSG", prop.getProp("log.err.msg"));
		}
		
		wemDao.insertLog(logMap);
		
		log.info("K11.시간통계_정시자료 정시데이터 수집 완료");
	}

	public static void getSnow1(String serverTime) {
		log.info("K33.일신적설 정시데이터 수집...");
		HashMap<String, String> parameter = new HashMap<String, String>();
		// 강수 정시자료
		parameter.put("authKey", APIKEY);
		parameter.put("tm", serverTime);
		parameter.put("sd", prop.getProp("kma.snow1.sd"));
		
		List<String> ret = HttpUtils.excuteGetConnection(URL2, parameter);
		List<Map<String, Object>> resultList = new ArrayList<Map<String,Object>>();
		String[] colNmArr = prop.getProp("kma.snow1.arr").split(",");
//		String sample = "202205021000,    90,             속초, 128.56472778,  38.25085068, 608-----,    0.0,=";
		
		for(String item : ret) {
			if(item.contains("#")) { continue; }
			else {
				String[] fieldValueArr = item.split(",");
				Map<String, Object> parsedMap = new HashMap<String, Object>();
				parsedMap = initializeMap(parsedMap, colNmArr);
				int colIdx = 0;
				for(String fieldValue : fieldValueArr) {
					String trimFieldVal = fieldValue.trim();
					if(trimFieldVal.contains("=")) { continue; }
					else {
						if(colIdx == 0) {
							parsedMap.put(colNmArr[colIdx++], trimFieldVal);
							parsedMap.put(colNmArr[colIdx++], trimFieldVal.substring(0, 8));
							parsedMap.put(colNmArr[colIdx++], trimFieldVal.substring(8, 12));
						} else {
							parsedMap.put(colNmArr[colIdx++], trimFieldVal);
						}
					}
				}
				resultList.add(parsedMap);
			}
		}
		
		log.info("size: " + resultList.size());
		int sqlIdx = 0;
		int resultCnt = 0;
		for(Map<String, Object> item : resultList) {
			log.info("{}번째 DATA 삽입중...", ++sqlIdx);
			int setCnt = wemDao.mergeSnow1Data(item);
			resultCnt += setCnt;
		}
		
		Map<String, Object> logMap = new HashMap<String, Object>();
		logMap.put("IF_ID", prop.getProp("esi.interface.snow1"));
		logMap.put("DATA_CNT", resultCnt);
		logMap.put("CRE_DTTM", new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis()));
		if(resultCnt > 0) {
			logMap.put("TRST_STAT", "C");
			logMap.put("ERR_MSG", "");
		} else {
			logMap.put("TRST_STAT", "E");
			logMap.put("ERR_MSG", prop.getProp("log.err.msg"));
		}
		
		wemDao.insertLog(logMap);
		
		log.info("K33.일신적설 정시데이터 수집 완료");
	}
	
	private static Map<String, Object> initializeMap(Map<String, Object> map, String[] colNmArr) {
		map.clear();
		for(String key : colNmArr) {
			map.put(key, "");
		}
		
		return map;
	}

	public static void updateAwshCode() {
		log.info("K11.시간통계 코드 정보 갱신...");
		String tableName = prop.getProp("table.awsh.name");
		wemDao.deleteCode(tableName);
		
		HashMap<String, String> parameter = new HashMap<String, String>();
		parameter.put("authKey", APIKEY);
		parameter.put("inf", prop.getProp("kma.awsh.code.inf"));
		List<String> ret = HttpUtils.excuteGetConnection(URL3, parameter);
		String[] colNmArr = prop.getProp("kma.code.awsh.arr").split(",");
//		String sample = "   90  128.56473000   38.25085000 35100        17.53     18.73      1.70     10.00      1.40  90 속초                 Sokcho               11D20401 4282033035 ----";
		
		List<Map<String, Object>> resultList = new ArrayList<Map<String,Object>>();
		for(String item : ret) {
			if(item.contains("#")) { continue; }
			else {
				String[] fieldValueArr = item.split(" ");
				Map<String, Object> parsedMap = new HashMap<String, Object>();
				parsedMap = initializeMap(parsedMap, colNmArr);
				
				List<String> parsedValueList = new ArrayList<String>();
				for(String fieldValue : fieldValueArr) {
					if(fieldValue.length() == 0) { continue; }
					else {
						parsedValueList.add(fieldValue);
					}
				}
				
				// 필드 값을 " "으로 구분하나, 필드 값 내부에 " "을 가지는 값이 있어 올바르게 파싱되고 있지 않음, 해당 케이스 분기처리
				int colIdx = 0;
				if (parsedValueList.size() == 15) {
					for(String fieldValue : parsedValueList) {
						parsedMap.put(colNmArr[colIdx++], fieldValue);
					}
				} else {
					String temp = "";
					for(int i = 0; i < parsedValueList.size(); i++) {
						if( i == 11 ) {
							temp += parsedValueList.get(i);
							continue;
						} else if( i == 12 ) {
							temp += parsedValueList.get(i);
							parsedMap.put(colNmArr[colIdx], temp);
						} else {
							parsedMap.put(colNmArr[colIdx], parsedValueList.get(i));
						}
						colIdx++;
					}
				}
				log.debug(parsedMap.toString());
				resultList.add(parsedMap);
			}
		}
		log.info("size: " + resultList.size());
		int sqlIdx = 0;
		for(Map<String, Object> item : resultList) {
			log.info("{}번째 DATA 삽입중...", ++sqlIdx);
			wemDao.instertCode(item, tableName);
		}
		
		log.info("K11.시간통계 코드 정보 갱신 완료");
	}

	public static void updateSnow1Code() {
		log.info("K33.일신적설 코드 정보 갱신...");
		String tableName = prop.getProp("table.snow1.name");
		wemDao.deleteCode(tableName);
		
		HashMap<String, String> parameter = new HashMap<String, String>();
		parameter.put("authKey", APIKEY);
		List<String> ret = HttpUtils.excuteGetConnection(URL4, parameter);
		String[] colNmArr = prop.getProp("kma.code.snow1.arr").split(",");
//		String sample = "   90  128.56473000   38.25085000 608          17.53 105 속초                 11D20401 4282033035";
		
		List<Map<String, Object>> resultList = new ArrayList<Map<String,Object>>();
		for(String item : ret) {
			if(item.contains("#")) { continue; }
			else {
				String[] fieldValueArr = item.split(" ");
				Map<String, Object> parsedMap = new HashMap<String, Object>();
				parsedMap = initializeMap(parsedMap, colNmArr);
				int colIdx = 0;
				for(String fieldValue : fieldValueArr) {
					if(fieldValue.length() == 0) { continue; }
					else {
						parsedMap.put(colNmArr[colIdx++], fieldValue);
					}
				}
				log.info(parsedMap.toString());
				resultList.add(parsedMap);
			}
		}
		log.info("size: " + resultList.size());
		int sqlIdx = 0;
		for(Map<String, Object> item : resultList) {
			log.info("{}번째 DATA 삽입중...", ++sqlIdx);
			wemDao.instertCode(item, tableName);
		}
		
		log.info("K33.일신적설 코드 정보 갱신 완료");
	}
}