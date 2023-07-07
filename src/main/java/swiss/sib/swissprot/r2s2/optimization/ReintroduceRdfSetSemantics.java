package swiss.sib.swissprot.r2s2.optimization;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.JdbcUtil;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.GroupOfColumns;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.SqlDatatype;
import swiss.sib.swissprot.r2s2.sql.Table;

public class ReintroduceRdfSetSemantics {
	private static final Logger log = LoggerFactory.getLogger(ReintroduceRdfSetSemantics.class);

	public static void optimize(Connection conn, Table table) {
		final boolean thereIsAtLeastOnePhysicalColumn = allColumns(table).findAny().isPresent();
		if (thereIsAtLeastOnePhysicalColumn) {
			try {
				String tempName = table.name() + "_temp";

				// if more than one graph per subject/object

				Map<Boolean, List<Column>> m = table.subject().columns().stream()
						.collect(Collectors.groupingBy(GroupOfColumns::isAGraphColumn));

				List<Column> nonGraphColumns = m.get(Boolean.FALSE);
				List<Column> graphColumns = m.get(Boolean.TRUE);
				boolean subjectsInOneGraph = seeIfSubjectsAreInMultipleGraphs(table.name(), conn,
						nonGraphColumns, graphColumns);
				if (!subjectsInOneGraph) {
					assert graphColumns.size() == 1; 
					graphColumns.get(0);
					
					createNewTableForOneSubjectMultipleGraphs(conn, table, tempName, nonGraphColumns, graphColumns);
				} else {
					createNewTableForOneSubjectOneGraph(conn, table, tempName);
				}

				renameTemporaryTableIntoFinalName(conn, table, tempName);
				JdbcUtil.commitIfNeeded(conn);
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	private static void renameTemporaryTableIntoFinalName(Connection conn, Table table, String tempName)
			throws SQLException {
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
	}

	private static void createNewTableForOneSubjectMultipleGraphs(Connection conn, Table table, String tempName, List<Column> nonGraphColumns, List<Column> graphColumns)
			throws SQLException {
		for (Column gc:graphColumns) {
			gc.setDatatype(SqlDatatype.GRAPH_IRIS_LIST);
		}
		String nonGraphColumnNames = nonGraphColumns.stream().map(Column::name).collect(Collectors.joining(", "));
		String graphColumnNames = graphColumns.stream().map(Column::name).collect(Collectors.joining(", "));
		try (Statement st = conn.createStatement()) {
			String columns = allColumns(table).map(Column::definition).collect(Collectors.joining(", "));
			final String sql = "CREATE TABLE " + tempName + "(" + columns + ")";
			log.info("Running:" + sql);
			st.execute(sql);
		}
		try (Statement st = conn.createStatement()) {
			String columns = allColumns(table).map(Column::name).collect(Collectors.joining(", "));
			final String sql = "INSERT INTO " + tempName + "(" + nonGraphColumnNames + ", LIST(DISTINCT "
					+ graphColumnNames + ")) SELECT DISTINCT " + columns + " FROM " + table.name() + "GROUP BY "
					+ nonGraphColumnNames + "  ORDER BY " + nonGraphColumnNames;
			log.info("Running:" + sql);
			st.execute(sql);
		}
	}
	
	private static void createNewTableForOneSubjectOneGraph(Connection conn, Table table, String tempName)
			throws SQLException {
		try (Statement st = conn.createStatement()) {
			String columns = allColumns(table).map(Column::definition).collect(Collectors.joining(", "));
			final String sql = "CREATE TABLE " + tempName + "(" + columns + ")";
			log.info("Running:" + sql);
			st.execute(sql);
		}
		try (Statement st = conn.createStatement()) {
			String columns = allColumns(table).map(Column::name).collect(Collectors.joining(", "));
			final String sql = "INSERT INTO " + tempName + "(" + columns + ") SELECT DISTINCT " + columns
					+ " FROM " + table.name() + " ORDER BY " + columns;
			log.info("Running:" + sql);
			st.execute(sql);
		}
	}

	private static boolean seeIfSubjectsAreInMultipleGraphs(String tableName, Connection conn,
			List<Column> nonGraphColumns, List<Column> graphColumns) throws SQLException {
		if (graphColumns == null || graphColumns.isEmpty()) {
			return true;
		}
		try (Statement st = conn.createStatement()) {
			String nonGraphColumnNames = nonGraphColumns.stream().map(Column::name).collect(Collectors.joining(", "));
			String graphColumnNames = graphColumns.stream().map(Column::name).collect(Collectors.joining(", "));
			
			String debugsql = "SELECT " + nonGraphColumnNames + ","+graphColumnNames+" FROM " + tableName + "";
			log.info("Running:" + debugsql  );
			try (ResultSet rs = st.executeQuery(debugsql)) {
				while (rs.next()) {
					log.info(rs.getString(4));
				}
			}
			
			String sql = "SELECT " + nonGraphColumnNames + " FROM " + tableName + " GROUP BY "
					+ nonGraphColumnNames + " HAVING (COUNT(DISTINCT " + graphColumnNames
					+ ") > 1)";
			log.info("Running:" + sql);
			boolean subjectsInOneGraph = true;
			try (ResultSet rs = st.executeQuery(sql)) {
				if (rs.next()) {
					log.info("No distinct graphs per subject.");
					subjectsInOneGraph = false;
				}
			}
			return subjectsInOneGraph;
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
