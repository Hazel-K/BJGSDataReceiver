package kr.co.ex.biz.WEM;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kr.co.ex.biz.JDBCTemplate.JDBCSelectTemplate;
import kr.co.ex.biz.JDBCTemplate.JDBCTemplateImpl;
import kr.co.ex.biz.JDBCTemplate.JDBCUpdateTemplate;
import kr.co.ex.biz.Util.AppProperties;
import kr.co.ex.biz.Util.DbUtils;

public class WEMDAO {
	private Logger log = LoggerFactory.getLogger(WEMDAO.class);
	private AppProperties prop = AppProperties.getInstance();
	private String CON = "";
	private String USER = "";
	private String SECRET = "";
	
	public WEMDAO() {
		String checkRunType = prop.getProp("application.runtype");
		String conStr = "sql." + checkRunType + ".con";
		String userStr = "sql." + checkRunType + ".user";
		String secretStr = "sql." + checkRunType + ".secret";
		
		CON = prop.getProp(conStr);
		USER = prop.getProp(userStr);
		SECRET = prop.getProp(secretStr);
	}
	
	private List<HashMap<String, Object>> selectMaxTranId() {
		final List<HashMap<String, Object>> result = new ArrayList<HashMap<String,Object>>();
		Connection con = DbUtils.getCon("ORACLE", CON, USER, SECRET);
		String sql = prop.getProp("sql.real.log.maxval");
		
		JDBCTemplateImpl.executeQuery(con, sql, new JDBCSelectTemplate() {
			public void prepared(PreparedStatement ps) throws SQLException {}
			
			public void executeQuery(ResultSet rs) throws SQLException {
				ResultSetMetaData metaData = rs.getMetaData();
				int colSize = metaData.getColumnCount();
				while(rs.next()) {
					HashMap<String, Object> map = new HashMap<String, Object>();
					for(int i = 0; i < colSize; i++) {
						String column = metaData.getColumnName(i + 1);
						map.put(column, rs.getString(column));
						
					}
					result.add(map);
				}
			}
		});
		
		return result;
	}

	public int mergeAwshData(final Map<String, Object> item) {
		Connection con = DbUtils.getCon("ORACLE", CON, USER, SECRET);
		String sql = prop.getProp("sql.real.awsh");
		
		int result =  JDBCTemplateImpl.executeUpdate(con, sql, new JDBCUpdateTemplate() {
			public void update(PreparedStatement ps) throws SQLException {
				String[] colNmArr = prop.getProp("sql.real.awsh.arr").split(",");
				int setIdx = 2 * colNmArr.length;
				int setColIdx = 0;
				
				for(int i = 0; i < setIdx; i++) {
				if(setColIdx == colNmArr.length) { setColIdx = 0; }
					String colNm = colNmArr[setColIdx++];
					int colIdx = i + 1;
					String value = String.valueOf(item.get(colNm));
					log.debug("idx: {}, setColIdx: {}, colNm: {}, value: {}", colIdx, setColIdx, colNm, value);
					ps.setString(colIdx, value);
				}
			}
		});
		
		return result;
	}

	public int mergeSnow1Data(final Map<String, Object> item) {
		Connection con = DbUtils.getCon("ORACLE", CON, USER, SECRET);
		String sql = prop.getProp("sql.real.snow1");
		
		int result = JDBCTemplateImpl.executeUpdate(con, sql, new JDBCUpdateTemplate() {
			public void update(PreparedStatement ps) throws SQLException {
				String[] colNmArr = prop.getProp("sql.real.snow1.arr").split(",");
				int setIdx = 2 * colNmArr.length;
				int setColIdx = 0;
				
				for(int i = 0; i < setIdx; i++) {
				if(setColIdx == colNmArr.length) { setColIdx = 0; }
					String colNm = colNmArr[setColIdx++];
					int colIdx = i + 1;
					String value = String.valueOf(item.get(colNm));
					log.debug("idx: {}, setColIdx: {}, colNm: {}, value: {}", colIdx, setColIdx, colNm, value);
					ps.setString(colIdx, value);
				}
			}
		});
		
		return result;
	}

	public void deleteCode(final String TABLE_NAME) {
		Connection con = DbUtils.getCon("ORACLE", CON, USER, SECRET);
		String sql = prop.getProp("sql.real.delete.code");
		
		JDBCTemplateImpl.executeUpdate(con, sql, new JDBCUpdateTemplate() {
			public void update(PreparedStatement ps) throws SQLException {
				ps.setString(1, TABLE_NAME);
			}
		});
	}

	public void instertCode(final Map<String, Object> item, final String tableName) {
		Connection con = DbUtils.getCon("ORACLE", CON, USER, SECRET);
		String sql = "";
		
		if(prop.getProp("table.awsh.name").equals(tableName)) {
			sql = prop.getProp("sql.real.awsh.code");
		} else if(prop.getProp("table.snow1.name").equals(tableName)) {
			sql = prop.getProp("sql.real.snow1.code");
		}
		
		JDBCTemplateImpl.executeUpdate(con, sql, new JDBCUpdateTemplate() {
			public void update(PreparedStatement ps) throws SQLException {
				String[] colNmArr = null;
				if(prop.getProp("table.awsh.name").equals(tableName)) {
					colNmArr = prop.getProp("sql.real.awsh.code.arr").split(",");
				} else if(prop.getProp("table.snow1.name").equals(tableName)) {
					colNmArr = prop.getProp("sql.real.snow1.code.arr").split(",");
				}
				
				int colIdx = 0;
				for(String key : colNmArr) {
					ps.setString(++colIdx, String.valueOf(item.get(key)));
				}
			}
		});
	}
	
	public void insertLog(final Map<String, Object> item) {
		Connection con = DbUtils.getCon("ORACLE", CON, USER, SECRET);
		String sql = prop.getProp("sql.real.log");
		
		List<HashMap<String, Object>> maxTranId = selectMaxTranId();
		String maxTrId = String.valueOf(maxTranId.get(0).get("TRAN_ID"));
		String extractedMaxValue = maxTrId.substring(maxTrId.length() - 7, maxTrId.length());
		int mutatedNum = Integer.parseInt(extractedMaxValue);
		
		String resultTranId = "";
		int tempLength = 7 - String.valueOf(mutatedNum).length();
		resultTranId += new SimpleDateFormat("yyyyMMddHHmmssSSS").format(System.currentTimeMillis());
		for(int i = 0; i < tempLength; i++) {
			resultTranId += "0";
		}
		resultTranId += ( mutatedNum + 1 );
		
		item.put("TRAN_ID", resultTranId);
		
		JDBCTemplateImpl.executeUpdate(con, sql, new JDBCUpdateTemplate() {
			public void update(PreparedStatement ps) throws SQLException {
				String[] colNmArr = prop.getProp("sql.real.log.arr").split(",");
				int colIdx = 0;
				for(String key : colNmArr) {
					ps.setString(++colIdx, String.valueOf(item.get(key)));
				}
			}
		});
	}
}