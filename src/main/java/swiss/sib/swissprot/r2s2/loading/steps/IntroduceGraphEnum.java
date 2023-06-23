package swiss.sib.swissprot.r2s2.loading.steps;

import static swiss.sib.swissprot.r2s2.JdbcUtil.openByJdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.JdbcUtil;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap.TempIriId;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.GroupOfColumns;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.SqlDatatype;
import swiss.sib.swissprot.r2s2.sql.Table;

public record IntroduceGraphEnum(String temp, List<Table> tables, TemporaryIriIdMap temporaryGraphIdMap) {

	private static final Logger logger = LoggerFactory.getLogger(IntroduceGraphEnum.class);
	public void run() {
		try (Connection conn_rw = openByJdbc(temp)) {

			try (java.sql.Statement stat = conn_rw.createStatement()) {
				stat.execute("CREATE TYPE " + SqlDatatype.GRAPH_IRIS.label()
						+ " AS ENUM (SELECT graphs.iri FROM graphs ORDER BY graphs.id)");
			} catch (SQLException e) {
				logger.info("graphs enum already exists");
				return;
			}

			for (Table table : tables) {
				table.objects().stream().map(PredicateMap::groupOfColumns).map(GroupOfColumns::columns)
						.flatMap(List::stream).filter(GroupOfColumns::isAGraphColumn).forEach(gc -> 
							alterTable(conn_rw, table, gc));

			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	public void alterTable(Connection conn_rw, Table table, Column graphColumn) {
		try (Statement stat = conn_rw.createStatement()) {
			final String cast = "ALTER TABLE " + table.name() + " ALTER " + graphColumn.name()
					+ " TYPE graph_iris USING (" + buildCase(graphColumn) + ")";
			logger.info("casting " + cast);
			stat.execute(cast);
			JdbcUtil.commitIfNeeded(conn_rw);
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	public String buildCase(Column graphColumn) {
		StringBuilder asCase = new StringBuilder("CASE");
		for (TempIriId id : temporaryGraphIdMap.iris()) {
			asCase.append(" WHEN ");
			asCase.append(graphColumn.name());
			asCase.append(" = ");
			asCase.append(id.id());
			asCase.append(" THEN " + "'");
			asCase.append(id.stringValue());
			asCase.append("'");
		}
		graphColumn.setDatatype(SqlDatatype.GRAPH_IRIS);
		asCase.append(" END");
		return asCase.toString();
	}
}
