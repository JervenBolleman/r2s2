package swiss.sib.swissprot.r2s2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcUtil {
	private static Logger logger = LoggerFactory.getLogger(JdbcUtil.class);

	private JdbcUtil () {
		
	}
	
	public static Connection open(String driver, String duckDbPath) throws SQLException {
		return DriverManager.getConnection("jdbc:"+driver+":" + duckDbPath, new Properties());
	}
	
	public static Connection openByJdbc(String jdbc) throws SQLException {
		return DriverManager.getConnection(jdbc, new Properties());
	}
	
	public static void checkpoint(Connection conn_rw) {
		try (java.sql.Statement s = conn_rw.createStatement()) {
			final boolean execute = s.execute("checkpoint");
//			assert execute;
		} catch (SQLException e) {
			logger.info("This database does not checkpoint");
		}
	}

	public static void commitIfNeeded(Connection conn_rw) throws SQLException {
		if (!conn_rw.getAutoCommit()) {
			conn_rw.commit();
		}
	}
}
