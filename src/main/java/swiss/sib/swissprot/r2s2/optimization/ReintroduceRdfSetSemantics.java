package swiss.sib.swissprot.r2s2.optimization;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.JdbcUtil;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.GroupOfColumns;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;

public class ReintroduceRdfSetSemantics {
	private static final Logger log = LoggerFactory.getLogger(ReintroduceRdfSetSemantics.class);

	public static void optimize(Connection conn, Table table) {
		final Stream<Column> concat = allColumns(table);
		final boolean thereIsAtLeastOnePhysicalColumn = concat.findAny().isPresent();
		if (thereIsAtLeastOnePhysicalColumn) {
			try {
				String tempName = table.name() + "_temp";
//				Table temp = new Table(table.name() + "_temp", table.subject(), table.subjectKind(), table.objects());
//				temp.create(conn);
				try (Statement st = conn.createStatement()) {
					String columns = allColumns(table).map(Column::definition).collect(Collectors.joining(", "));
					final String sql = "CREATE TABLE " + tempName + "(" + columns + ")";
					log.info("Running:" + sql);
					st.execute(sql);
				}
				try (Statement st = conn.createStatement()) {
					String columns = allColumns(table).map(Column::name).collect(Collectors.joining(", "));
					final String sql = "INSERT INTO " + tempName + "(" + columns + ") SELECT DISTINCT "+columns+" FROM "
							+ table.name() + " ORDER BY "+columns;
					log.info("Running:" + sql);
					st.execute(sql);
				}
				try (Statement st = conn.createStatement()) {
					final String sql = "DROP TABLE " + table.name();
					log.info("Running:" + sql);
					st.execute(sql);
				}
				try (Statement st = conn.createStatement()) {
					final String sql = "ALTER TABLE " + tempName + " RENAME TO " + table.name();
					log.info("Running:" + sql);
					st.execute(sql);
				}
				JdbcUtil.commitIfNeeded(conn);
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	public static Stream<Column> allColumns(Table table) {
		final Stream<Column> subjectColums = table.subject().columns().stream();
		final Stream<Column> objectColums = table.objects().stream().map(PredicateMap::groupOfColumns)
				.map(GroupOfColumns::columns).flatMap(List::stream);
		final Stream<Column> concat = Stream.concat(subjectColums, objectColums);
		return concat.filter(Column::isPhysical);
	}
}
