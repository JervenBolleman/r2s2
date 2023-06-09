package swiss.sib.swissprot.r2s2.optimization;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.JdbcUtil;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;
import swiss.sib.swissprot.r2s2.sql.VirtualSingleValueColumn;

public class IntroduceVirtualColumns {
	private static final Logger log = LoggerFactory.getLogger(IntroduceVirtualColumns.class);

	public static void optimize(Connection conn, Table table) {

		replaceSingleValueColumnsWithVirtual(table, table.subject().columns(), conn);
		for (PredicateMap p : table.objects()) {
			replaceSingleValueColumnsWithVirtual(table, p.groupOfColumns().columns(), conn);
		}
	}

	private static void replaceSingleValueColumnsWithVirtual(Table table, List<Column> columns, Connection conn) {

		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i);
			if (!column.isVirtual()) {
				try (Statement ct = conn.createStatement()) {
					String dc = "SELECT DISTINCT " + column.name() + " FROM " + table.name() + " LIMIT 2";
					log.warn("Running: " + dc);
					try (ResultSet executeQuery = ct.executeQuery(dc)) {
						boolean first = executeQuery.next();
						if (!first) {
							log.info(table.name() + '.' + column.name() + " is a null valued column");
							replaceAColumn(table, columns, conn, i, column, null);
						} else {
							String value = executeQuery.getString(1);
							if (!executeQuery.next()) {
								replaceAColumn(table, columns, conn, i, column, value);
							} else {
								log.info(table.name() + '.' + column.name() + " has more than one value");
							}
						}
					}
				} catch (SQLException e) {
					throw new IllegalStateException(e);
				}
			}
		}
	}

	private static void replaceAColumn(Table table, List<Column> columns, Connection conn, int i, Column column,
			String value) throws SQLException {
		log.info(table.name() + '.' + column.name() + " has one value:"+value);
		columns.set(i, new VirtualSingleValueColumn(column.name(), column.sqlDatatype(), value));
		try (Statement ct2 = conn.createStatement()) {
			String dropColumn = "ALTER TABLE " + table.name() + " DROP " + column.name();
			log.info("dropping: " + table.name() + "." + column.name());
			ct2.execute(dropColumn);
			JdbcUtil.commitIfNeeded(conn);
		} catch (SQLException e) {
			// Last column can not be dropped. So we just delete all values.
			try (Statement ct3 = conn.createStatement()) {
				final String emptyTable = "DELETE FROM " + table.name();
				log.info("emptying: " + table.name() + " " + emptyTable);
				ct3.execute(emptyTable);
				JdbcUtil.commitIfNeeded(conn);
			}
		}
	}
}
