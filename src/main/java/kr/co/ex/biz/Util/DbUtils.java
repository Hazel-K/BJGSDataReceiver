package kr.co.ex.biz.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbUtils {
	private static final Logger log = LoggerFactory.getLogger(DbUtils.class);
	private static final String ORACLEDRIVER = "oracle.jdbc.driver.OracleDriver";
	
	public static Connection getCon(String type, String url, String user, String secret) {
		Connection con = null;
		if("ORACLE".equals(type)) {
			try {
				Class.forName(ORACLEDRIVER);
			} catch(ClassNotFoundException e) {
				log.error("OJDBC Oracle Driver를 찾을 수 없음");
			}			
		}
		
		try {
			con = DriverManager.getConnection(url, user, secret);
		} catch (SQLException e) {
			log.error("SQL Connection을 얻을 수 없음");
		}
		return con;
	}
	
	public static void close(Connection con, PreparedStatement ps) {
		try {
			if(ps != null && !ps.isClosed()) { ps.close(); }
			if(con != null && !con.isClosed()) { con.close(); }
		} catch (SQLException e) {
			log.error("SQL con, ps을 닫을 수 없음");
		}
	}
	
	public static void close(Connection con, PreparedStatement ps, ResultSet rs) {
		try {
			if(rs != null && !rs.isClosed()) { rs.close(); }
		} catch (SQLException e) {
			log.error("SQL rs를 닫을 수 없음");
		}
		close(con, ps);
	}
}
