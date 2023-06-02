package swiss.sib.swissprot.r2s2.analysis;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.Datatypes;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;
import swiss.sib.swissprot.r2s2.sql.VirtualSingleValueColumn;

public class IntroduceVirtualColumns {
	private static final Logger log = LoggerFactory.getLogger(IntroduceVirtualColumns.class);

	public static void optimizeForR2RML(Connection conn, Table table) throws SQLException {

		replaceSingleValueColumnsWithVirtual(table, table.subject().getColumns(), conn);
		replaceLongestStartingPrefixWithVirtual(table, table.subject().getColumns(), conn);
		for (PredicateMap p : table.objects()) {
			replaceSingleValueColumnsWithVirtual(table, p.columns().getColumns(), conn);
			replaceLongestStartingPrefixWithVirtual(table, p.columns().getColumns(), conn);
		}
	}

	private static void replaceLongestStartingPrefixWithVirtual(Table table, List<Column> columns, Connection conn)
			throws SQLException {
		int max = columns.size();
		for (int i = 0; i < max; i++) {
			Column column = columns.get(i);
			if (!column.isVirtual()) {
				String lcs = findLongestCommonPrefixString(table, conn, column);
				if (lcs != null) {
					columns.add(columns.indexOf(column),
							new VirtualSingleValueColumn(column.name() + "_lcs", column.datatype(), lcs));
					try (Statement ct = conn.createStatement()) {
						String uc = "UPDATE " + table.name() + " SET " + column.name() + "= SUBSTRING(" + column.name()
								+ ",0," + lcs.length() + ')';
						log.warn(uc);
						
						ct.executeUpdate(uc);
						if(!conn.getAutoCommit()) {
							conn.commit();
						}
					}
				}
			}
		}
	}

	private static String findLongestCommonPrefixString(Table table, Connection conn, Column column)
			throws SQLException {
		if (!column.isVirtual() && Datatypes.TEXT == column.datatype()) {
			try (Statement ct = conn.createStatement()) {
				String dc = "SELECT " + column.name() + " FROM " + table.name() + "";

				try (ResultSet executeQuery = ct.executeQuery(dc)) {
					boolean first = executeQuery.next();
					assert first;
					String value = executeQuery.getString(1);
					int pmax = value.length();
					while (executeQuery.next()) {
						String next = executeQuery.getString(1);

						int max = Math.min(next.length(), pmax);
						int csl = 0;
						while (csl < max && next.charAt(csl) == value.charAt(csl)) {
							csl++;
						}
						pmax = csl;
						if (pmax == 0) {
							return null;
						}
					}
					String lcs = value.substring(0, pmax);
					log.warn("Longest common substring for " + table.name() + '.' + column.name() + " is " + lcs);
					return lcs;
				}
			}
		}
		return null;
	}

	private static void replaceSingleValueColumnsWithVirtual(Table table, List<Column> columns, Connection conn)
			throws SQLException {

		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i);
			if (!column.isVirtual()) {
				try (Statement ct = conn.createStatement()) {
					String dc = "SELECT DISTINCT " + column.name() + " FROM " + table.name() + " LIMIT 2";

					log.warn("Running: " + dc);
					try (ResultSet executeQuery = ct.executeQuery(dc)) {
						boolean first = executeQuery.next();
						assert first;

						String value = executeQuery.getString(1);
						if (!executeQuery.next()) {
							log.info(table.name() + '.' + column.name() + " has one value");
							columns.set(i, new VirtualSingleValueColumn(column.name(), column.datatype(), value));
							try (Statement ct2 = conn.createStatement()) {
								String dropColumn = "ALTER TABLE " + table.name() + " DROP " + column.name();
								log.info("dropping: " + table.name() + "." + column.name());
								ct2.execute(dropColumn);
							}
						} else {
							log.info(table.name() + '.' + column.name() + " has more than one value");
						}
					}
				}
			}
		}
	}
}
