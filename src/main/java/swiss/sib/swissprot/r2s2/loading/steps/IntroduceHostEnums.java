package swiss.sib.swissprot.r2s2.loading.steps;

import static swiss.sib.swissprot.r2s2.JdbcUtil.openByJdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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

public record IntroduceHostEnums(String temp, List<Table> tables) {

	private static final Logger logger = LoggerFactory.getLogger(IntroduceHostEnums.class);

	public void run() {
		try (Connection conn_rw = openByJdbc(temp)) {

			Set<String> hosts = tables.stream().flatMap(table -> collectHostParts(conn_rw, table))
					.collect(Collectors.toSet());
			if (!hosts.isEmpty()) {
				String findDistinctHosts = hosts.stream().collect(Collectors.joining(" UNION ", "(", ")"));

				try (java.sql.Statement stat = conn_rw.createStatement()) {
					final String sql = "CREATE TYPE " + SqlDatatype.HOST.label() + " AS ENUM (SELECT DISTINCT * FROM ("
							+ findDistinctHosts + "))";
					logger.info("creating protocol part: " + sql);
					stat.execute(sql);
					tables.stream().forEach(table -> adaptHostParts(conn_rw, table));
				}
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	public Stream<String> collectHostParts(Connection conn_rw, Table table) {
		return table.objects().stream().map(PredicateMap::groupOfColumns).map(GroupOfColumns::columns).flatMap(List::stream)
				.filter(Column::isPhysical).filter(c -> c.name().endsWith(GroupOfColumns.HOST))
				.map(c -> "SELECT DISTINCT " + c.name() + " FROM " + table.name() + " WHERE "+c.name()+ " IS NOT NULL");
	}

	public void adaptHostParts(Connection conn_rw, Table table) {
		final Iterator<Column> iterator = table.objects().stream().map(PredicateMap::groupOfColumns).map(GroupOfColumns::columns)
				.flatMap(List::stream).filter(Column::isPhysical).filter(c -> c.name().endsWith(GroupOfColumns.HOST))
				.iterator();
		while (iterator.hasNext()) {
			Column hostColumn = iterator.next();
			try (java.sql.Statement stat = conn_rw.createStatement()) {
				final String cast = "ALTER TABLE " + table.name() + " ALTER " + hostColumn.name() + " TYPE "
						+ SqlDatatype.HOST.label();
				logger.info("casting " + cast);
				stat.execute(cast);
				JdbcUtil.commitIfNeeded(conn_rw);
				hostColumn.setDatatype(SqlDatatype.HOST);
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}
	}
}
