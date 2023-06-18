package swiss.sib.swissprot.r2s2.optimization;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.DuckDBUtil;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.GroupOfColumns;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;

public class ReintroduceRdfSetSemantics {
	private static final Logger log = LoggerFactory.getLogger(ReintroduceRdfSetSemantics.class);

	public static void optimize(Connection conn, Table table) {
		final Stream<Column> subjectColums = table.subject().columns().stream();
		final Stream<Column> objectColums = table.objects().stream().map(PredicateMap::groupOfColumns).map(GroupOfColumns::columns)
				.flatMap(List::stream);
		final boolean thereIsAtLeastOnePhysicalColumn = Stream.concat(subjectColums, objectColums)
				.filter(Column::isPhysical).findAny().isPresent();
		if (thereIsAtLeastOnePhysicalColumn) {
			try {
				try (Statement st = conn.createStatement()) {
					final String sql = "CREATE TABLE " + table.name() + "_temp AS SELECT DISTINCT * FROM " + table.name() + " ORDER BY *";
					log.info("Running:" + sql);
					st.execute(sql);
				}
				try (Statement st = conn.createStatement()) {
					final String sql = "DROP TABLE " + table.name();
					log.info("Running:" + sql);
					st.execute(sql);
				}
				try (Statement st = conn.createStatement()) {
					final String sql = "ALTER TABLE " + table.name() + "_temp RENAME TO " + table.name();
					log.info("Running:" + sql);
					st.execute(sql);
				}
				DuckDBUtil.commitIfNeeded(conn);
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}
	}
}
