package kr.co.ex.biz.JDBCTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import kr.co.ex.biz.Util.DbUtils;

public class JDBCTemplateImpl {
	public static int executeUpdate(Connection con, String sql, JDBCUpdateTemplate jdbc) {
		int result = 0;
		PreparedStatement ps = null;
		
		try {
			ps = con.prepareStatement(sql);
			jdbc.update(ps);
			result = ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbUtils.close(con, ps);
		}
		
		return result;
	}
	
	public static void executeQuery(Connection con, String sql, JDBCSelectTemplate jdbc) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			ps = con.prepareStatement(sql);
			jdbc.prepared(ps);
			rs = ps.executeQuery();
			jdbc.executeQuery(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbUtils.close(con, ps, rs);
		}
	}
}
