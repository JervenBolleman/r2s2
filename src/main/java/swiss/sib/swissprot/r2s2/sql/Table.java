package swiss.sib.swissprot.r2s2.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap.TempIriId;

public class Table {
	private static final Logger log = LoggerFactory.getLogger(Table.class);
	private final Columns subject;
	private final TempIriId predicate;
	private final Columns object;
	private final Column graphColumn;
	private final Kind subjectKind;
	private final Kind objectKind;

	public Table(TempIriId predicate, Columns subject, Kind subjectKind, Columns object, Kind objectKind,
			Column graphColumn) {
		super();
		this.predicate = predicate;
		this.subject = subject;
		this.subjectKind = subjectKind;
		this.object = object;
		this.objectKind = objectKind;
		this.graphColumn = graphColumn;
	}

	public Columns subject() {
		return subject;
	}

	public Columns object() {
		return object;
	}

	public void create(Connection conn) throws SQLException {
		try (Statement ct = conn.createStatement()) {
			String dml = "CREATE OR REPLACE TABLE " + name() + " (" + subject.definition() + ", " + object.definition()
					+ ", " + graphColumn.definition() + ")";
			LoggerFactory.getLogger(this.getClass()).warn("Running: " + dml);
			ct.execute(dml);
		}

	}

	public String name() {
		return "p_" + predicate.id() + "_" + subjectKind.label() + "_" + objectKind.label();
	}

	public Column graph() {
		return graphColumn;
	}

	public void optimizeForR2RML(Connection conn) throws SQLException {

		replaceSingleValueColumnsWithVirtual(subject.getColumns(), conn);
		replaceSingleValueColumnsWithVirtual(object.getColumns(), conn);
		repplaceLongestStartingPrefixWithVirtual(subject.getColumns(), conn);
		repplaceLongestStartingPrefixWithVirtual(object.getColumns(), conn);
	}

	private void repplaceLongestStartingPrefixWithVirtual(List<Column> columns, Connection conn) throws SQLException {
		int max = columns.size();
		for (int i = 0; i < max; i++) {
			Column column = columns.get(i);
			String lcs = findLongestCommonPrefixString(conn, column);
			if (lcs != null) {
				columns.add(columns.indexOf(column), new VirtualSingleValueColumn(column.name() + "_lcs", column.datatype(), lcs));
				try (Statement ct = conn.createStatement()) {
					String uc = "UPDATE " + name() + " SET " + column.name() + "= SUBSTRING(" + column.name() + ",0,"
							+ lcs.length() + ')';
					log.warn(uc);
					ct.executeUpdate(uc);
				}
			}
		}

	}

	private String findLongestCommonPrefixString(Connection conn, Column column) throws SQLException {
		if (!column.isVirtual() && Datatypes.TEXT == column.datatype()) {
			try (Statement ct = conn.createStatement()) {
				String dc = "SELECT " + column.name() + " FROM " + name() + "";

				try (ResultSet executeQuery = ct.executeQuery(dc)) {
					boolean first = executeQuery.next();
					assert first;
					String value = executeQuery.getString(1);
					int pmax = value.length();
					while (executeQuery.next()) {
						String next = executeQuery.getString(1);
						if (next.length() > pmax)
							pmax = next.length();

						int max = Math.min(next.length(), pmax);
						int csl = 0;
						while (csl < max && next.charAt(csl) == value.charAt(csl)) {
							csl++;
						}
						pmax = csl;
					}
					String lcs = value.substring(0, pmax);
					log.warn("Longest common substring for " + name() + '.' + column.name() + " is " + lcs);
					return lcs;
				}
			}
		}
		return null;
	}

	private void replaceSingleValueColumnsWithVirtual(List<Column> columns, Connection conn) throws SQLException {

		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i);
			try (Statement ct = conn.createStatement()) {
				if (Datatypes.TEXT == column.datatype()) {
					String dc = "SELECT DISTINCT " + column.name() + " FROM " + name() + " LIMIT 2";

					log.warn("Running: " + dc);
					try (ResultSet executeQuery = ct.executeQuery(dc)) {
						boolean first = executeQuery.next();
						assert first;

						String value = executeQuery.getString(1);
						if (!executeQuery.next()) {
							log.info(name() + '.' + column.name() + " has one value");
							columns.set(i, new VirtualSingleValueColumn(column.name(), column.datatype(), value));
							try (Statement ct2 = conn.createStatement()) {
								String dropColumn = "ALTER TABLE " + name() + " DROP " + column.name();
								log.info("dropping: " + name() + "." + column.name());
								ct2.execute(dropColumn);
							}
						}
					}
				}
			}
		}
	}
}
