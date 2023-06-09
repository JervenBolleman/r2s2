package swiss.sib.swissprot.r2s2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DuckDBUtil {
	private DuckDBUtil () {
		
	}
	
	public static Connection open(String duckDbPath) throws SQLException {
		return DriverManager.getConnection("jdbc:duckdb:" + duckDbPath, new Properties());
	}
	
	public static void checkpoint(Connection conn_rw) throws SQLException {
		try (java.sql.Statement s = conn_rw.createStatement()) {
			final boolean execute = s.execute("checkpoint");
			assert execute;
		}
	}

	public static void commitIfNeeded(Connection conn_rw) throws SQLException {
		if (!conn_rw.getAutoCommit()) {
			conn_rw.commit();
		}
	}
}
