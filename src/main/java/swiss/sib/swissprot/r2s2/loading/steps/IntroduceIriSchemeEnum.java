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

public record IntroduceIriSchemeEnum(String temp, List<Table> tables) {

	private static final Logger logger = LoggerFactory.getLogger(IntroduceIriSchemeEnum.class);

	public void run() {
		try (Connection conn_rw = openByJdbc(temp)) {
			Set<String> protocols = tables.stream().flatMap(table -> collectSchemeParts(conn_rw, table))
					.collect(Collectors.toSet());
			//Can be the case if all are virtual
			if (!protocols.isEmpty()) {
				String findDistinctProtocols = protocols.stream().collect(Collectors.joining(" UNION ", "(", ")"));

				try (java.sql.Statement stat = conn_rw.createStatement()) {
					final String sql = "CREATE TYPE " + SqlDatatype.SCHEME.label()
							+ " AS ENUM (SELECT DISTINCT * FROM (" + findDistinctProtocols + "))";
					logger.info("creating protocol part: " + sql);
					stat.execute(sql);
					tables.stream().forEach(table -> adaptSchemeParts(conn_rw, table));
				}
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}

	}

	public Stream<String> collectSchemeParts(Connection conn_rw, Table table) {
		return table.objects().stream().map(PredicateMap::groupOfColumns).map(GroupOfColumns::columns).flatMap(List::stream)
				.filter(Column::isPhysical).filter(c -> c.name().endsWith(GroupOfColumns.SCHEME))
				.map(c -> "SELECT DISTINCT " + c.name() + " FROM " + table.name() + " WHERE "+c.name()+ " IS NOT NULL");
	}

	public void adaptSchemeParts(Connection conn_rw, Table table) {
		final Iterator<Column> iterator = table.objects().stream().map(PredicateMap::groupOfColumns).map(GroupOfColumns::columns)
				.flatMap(List::stream).filter(Column::isPhysical).filter(c -> c.name().endsWith(GroupOfColumns.SCHEME))
				.iterator();
		while (iterator.hasNext()) {
			Column protocolColumn = iterator.next();
			try (java.sql.Statement stat = conn_rw.createStatement()) {
				final String cast = "ALTER TABLE " + table.name() + " ALTER " + protocolColumn.name() + " TYPE "
						+ SqlDatatype.SCHEME.label();
				logger.info("casting " + cast);
				stat.execute(cast);
				JdbcUtil.commitIfNeeded(conn_rw);
				protocolColumn.setDatatype(SqlDatatype.SCHEME);
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}
	}
}
