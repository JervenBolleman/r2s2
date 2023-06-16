package swiss.sib.swissprot.r2s2.loading.steps;

import static swiss.sib.swissprot.r2s2.DuckDBUtil.checkpoint;
import static swiss.sib.swissprot.r2s2.DuckDBUtil.open;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record Vacuum(String temp, String destination) {
	private static final Logger logger = LoggerFactory.getLogger(Vacuum.class);
	public void run() {
		Set<String> tablesToCopy;
		try {
			logger.info("finding tables that need to be in final copy");
			try (Connection conn_rw = open(temp)) {
				
				tablesToCopy = findAllPresentTables(conn_rw);
			}
			try (Connection conn_rw2 = open(destination)) {
				removeAnySystemTables(tablesToCopy, conn_rw2);
			}
			logger.info("running poor mans vacuum");
			for (String tableName : tablesToCopy) {
				try (Connection conn_rw2 = open(destination)) {
					try (Statement statement = conn_rw2.createStatement()) {
						statement.execute("ATTACH '" + temp + "' AS source (READ_ONLY)");
					}
					try (Statement statement = conn_rw2.createStatement()) {
						statement.execute("CREATE TABLE " + tableName + " AS SELECT * from source.main." + tableName);
					}
					checkpoint(conn_rw2);
				}
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
		try {
			logger.info("deleting temporary database");
			Files.delete(new File(temp).toPath());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void removeAnySystemTables(Set<String> tablesToCopy, Connection conn_rw2) throws SQLException {
		try (Statement statement = conn_rw2.createStatement()) {
			try (ResultSet rs = statement.executeQuery("SELECT table_name FROM information_schema.tables")) {
				while (rs.next()) {
					tablesToCopy.remove(rs.getString(1));
				}
			}
		}
	}

	private Set<String> findAllPresentTables(Connection conn_rw) throws SQLException {
		Set<String> tablesToCopy = new HashSet<>();
		try (Statement statement = conn_rw.createStatement()) {
			try (ResultSet rs = statement.executeQuery("SELECT table_name FROM information_schema.tables")) {
				while (rs.next()) {
					tablesToCopy.add(rs.getString(1));
				}
			}
		}
		return tablesToCopy;
	}
}
