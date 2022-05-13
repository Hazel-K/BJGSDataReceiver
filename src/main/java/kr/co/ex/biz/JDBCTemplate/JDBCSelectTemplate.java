package kr.co.ex.biz.JDBCTemplate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface JDBCSelectTemplate {
	void prepared(PreparedStatement ps) throws SQLException;
	void executeQuery(ResultSet rs) throws SQLException;
}
