package swiss.sib.swissprot.r2s2.loading.steps;

import static swiss.sib.swissprot.r2s2.DuckDBUtil.open;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.DuckDBUtil;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.Columns;
import swiss.sib.swissprot.r2s2.sql.Datatypes;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;

public record IntroduceHostEnums(String temp, List<Table> tables, TemporaryIriIdMap temporaryGraphIdMap) {

	private static final Logger logger = LoggerFactory.getLogger(IntroduceHostEnums.class);

	public void run() {
		try (Connection conn_rw = open(temp)) {
			Set<String> protocols = new HashSet<>();
			String findDistinctHosts = tables.stream().flatMap(table -> collectHostParts(conn_rw, protocols, table))
					.collect(Collectors.joining(" UNION ", "(", ")"));

			try (java.sql.Statement stat = conn_rw.createStatement()) {
				final String sql = "CREATE TYPE " + Datatypes.HOST.label() + " AS ENUM (SELECT DISTINCT * FROM ("
						+ findDistinctHosts + "))";
				logger.info("creating protocol part: " + sql);
				stat.execute(sql);
				tables.stream().forEach(table -> adaptProtocolParts(conn_rw, table));
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	public Stream<String> collectHostParts(Connection conn_rw, Set<String> protocols, Table table) {
		return table.objects().stream().map(PredicateMap::columns).map(Columns::getColumns).flatMap(List::stream)
				.filter(Column::isPhysical).filter(c -> c.name().endsWith(Columns.HOST))
				.map(c -> "SELECT DISTINCT " + c.name() + " FROM " + table.name());
	}

	public void adaptProtocolParts(Connection conn_rw, Table table) {
		final Iterator<Column> iterator = table.objects().stream().map(PredicateMap::columns).map(Columns::getColumns)
				.flatMap(List::stream).filter(Column::isPhysical).filter(c -> c.name().endsWith(Columns.HOST))
				.iterator();
		while (iterator.hasNext()) {
			Column hostColumn = iterator.next();
			try (java.sql.Statement stat = conn_rw.createStatement()) {
				final String cast = "ALTER TABLE " + table.name() + " ALTER " + hostColumn.name() + " TYPE "
						+ Datatypes.HOST.label();
				logger.info("casting " + cast);
				stat.execute(cast);
				DuckDBUtil.commitIfNeeded(conn_rw);
				hostColumn.setDatatype(Datatypes.HOST);
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}
	}
}
