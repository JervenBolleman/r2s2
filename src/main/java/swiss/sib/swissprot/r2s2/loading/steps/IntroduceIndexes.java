package swiss.sib.swissprot.r2s2.loading.steps;

import static swiss.sib.swissprot.r2s2.JdbcUtil.openByJdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.JdbcUtil;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.Table;

public record IntroduceIndexes(String temp, List<Table> tables) {

	private static final Logger logger = LoggerFactory.getLogger(IntroduceIndexes.class);

	public void run() {
		for (Table table : tables)
			try (Connection conn_rw = openByJdbc(temp)) {
				final String subjColumns = table.subject().columns().stream().filter(Column::isPhysical)
						.map(Column::name).collect(Collectors.joining(", "));
				if (!subjColumns.isEmpty())
					try (Statement stat = conn_rw.createStatement()) {
						final String createIdx = "CREATE UNIQUE INDEX " + table.name() + "_subj_idx ON " + table.name()
								+ " (" + subjColumns + ")";
						logger.info("creating index " + createIdx);
						stat.execute(createIdx);
						JdbcUtil.commitIfNeeded(conn_rw);
					} catch (SQLException e) {
						try (Statement stat = conn_rw.createStatement()) {
							final String createIdx = "CREATE INDEX " + table.name() + "_subj_idx ON " + table.name()
									+ " (" + subjColumns + ")";
							logger.info("creating index " + createIdx);
							stat.execute(createIdx);
							JdbcUtil.commitIfNeeded(conn_rw);
						}
					}
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
	}
}
