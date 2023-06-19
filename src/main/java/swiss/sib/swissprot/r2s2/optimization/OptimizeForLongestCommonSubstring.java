package swiss.sib.swissprot.r2s2.optimization;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.JdbcUtil;
import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.SqlDatatype;
import swiss.sib.swissprot.r2s2.sql.Table;
import swiss.sib.swissprot.r2s2.sql.VirtualSingleValueColumn;

public class OptimizeForLongestCommonSubstring {
	private static Logger log = LoggerFactory.getLogger(OptimizeForLongestCommonSubstring.class);

	public static void optimize(Connection conn, Table table) {
		if (table.subjectKind() == Kind.IRI) {
			replaceLongestStartingPrefixWithVirtual(table, table.subject().columns(), conn);
		}
		for (PredicateMap p : table.objects()) {
			if (p.objectKind() == Kind.IRI)
				replaceLongestStartingPrefixWithVirtual(table, p.groupOfColumns().columns(), conn);
		}
	}

	private static void replaceLongestStartingPrefixWithVirtual(Table table, List<Column> columns, Connection conn) {
		int max = columns.size();
		for (int i = 0; i < max; i++) {
			Column column = columns.get(i);
			if (column.isPhysical()) {
				try {
					String lcs = findLongestCommonPrefixString(table, conn, column);
					if (lcs != null) {
						columns.add(columns.indexOf(column),
								new VirtualSingleValueColumn(column.name() + "_lcs", column.sqlDatatype(), lcs));
						try (Statement ct = conn.createStatement()) {
							String uc = "UPDATE " + table.name() + " SET " + column.name() + " = SUBSTRING("
									+ column.name() + "," + (lcs.length() + 1) + ")";
							log.warn(uc);

							ct.executeUpdate(uc);
							JdbcUtil.commitIfNeeded(conn);
						}
					}
				} catch (SQLException e) {
					throw new IllegalStateException(e);
				}
			}
		}
	}

	private static String findLongestCommonPrefixString(Table table, Connection conn, Column column)
			throws SQLException {
		if (!column.isVirtual() && SqlDatatype.TEXT == column.sqlDatatype()) {
			try (Statement ct = conn.createStatement()) {
				String dc = "SELECT " + column.name() + " FROM " + table.name() + " WHERE " + column.name()
						+ " IS NOT NULL";
				log.info("Running " + dc);
				try (ResultSet executeQuery = ct.executeQuery(dc)) {
					boolean first = executeQuery.next();
					assert first;
					String value = executeQuery.getString(1);
					int pmax = value.length();
					while (executeQuery.next()) {
						String next = executeQuery.getString(1);
						if (next != null) {
							// After table merging some fields might be null and that is ok
							// they need to be skipped.
							int max = Math.min(next.length(), pmax);
							pmax = sharedSubStringStart(value, next, max);
							if (pmax == 0) {
								return null;
							}
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

	public static int sharedSubStringStart(String value, String next, int max) {
		int pmax;
		int csl = 0;
		while (csl < max && next.charAt(csl) == value.charAt(csl)) {
			csl++;
		}
		pmax = csl;
		return pmax;
	}
}
