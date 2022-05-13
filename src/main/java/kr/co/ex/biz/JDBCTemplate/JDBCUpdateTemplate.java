package kr.co.ex.biz.JDBCTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface JDBCUpdateTemplate {
	void update(PreparedStatement ps) throws SQLException;
}
