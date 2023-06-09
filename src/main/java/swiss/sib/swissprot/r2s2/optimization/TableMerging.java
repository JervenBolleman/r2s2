package swiss.sib.swissprot.r2s2.optimization;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.JdbcUtil;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;

public class TableMerging {
	private static final Logger log = LoggerFactory.getLogger(TableMerging.class);
	private final Connection conn;
	private final List<Table> tables;

	public TableMerging(Connection conn, List<Table> tables) {
		this.conn = conn;
		this.tables = tables;
	}

	public List<Table> run() throws SQLException {
		List<Table> mergeCandidates = new ArrayList<>();
		List<Table> notMerged = new ArrayList<>();
		for (Iterator<Table> iterator = tables.iterator(); iterator.hasNext();) {
			Table t = iterator.next();
			long size = size(t, conn);
			final List<Column> physicalColumns = physicalColumns(t);
			if (!physicalColumns.isEmpty()) {
				long distinctSubject = distinctSubject(t, conn, physicalColumns);
				if (size == distinctSubject) {
					mergeCandidates.add(t);
				} else {
					notMerged.add(t);
				}
			} else {
				notMerged.add(t);
			}
		}
		Map<List<Column>, List<Table>> collect = mergeCandidates.stream()
				.collect(Collectors.groupingBy((t) -> t.subject().columns()));
		List<Table> merged = new ArrayList<>();
		for (var en : collect.entrySet()) {
			var tablesToMerge = en.getValue();
			if (tablesToMerge.size() > 1) {
				en.getValue().sort((a, b) -> Long.compare(size(a, conn), size(b, conn)));
				var tablesToMergeIter = tablesToMerge.iterator();
				assert tablesToMergeIter.hasNext();
				var tableToMergeInto = tablesToMergeIter.next();
				merged.add(tableToMergeInto);
				while (tablesToMergeIter.hasNext()) {
					merge(tableToMergeInto, tablesToMergeIter.next(), conn);
					tablesToMergeIter.remove();
				}
			} else {
				notMerged.addAll(tablesToMerge);
			}
		}
		notMerged.addAll(merged);
		return notMerged;
	}

	private void merge(Table tableToMergeInto, Table next, Connection conn) throws SQLException {
		log.info("Merging " + next.name() + " into " + tableToMergeInto.name());
		try (var stat = conn.createStatement()) {
			for (PredicateMap pm : next.objects()) {
				List<Column> toMerge = new ArrayList<>();
				for (Column oc : pm.groupOfColumns().columns()) {
					if (oc.isPhysical()) {
						String alter = "ALTER TABLE " + tableToMergeInto.name() + " ADD COLUMN " + oc.definition();
						log.info(alter);
						stat.execute(alter);
						toMerge.add(oc);
					}
				}

				String listOfInsert = Stream.concat(next.subject().columns().stream(), toMerge.stream())
						.filter(c -> !c.isVirtual()).map(c -> c.name()).collect(Collectors.joining(", "));

				String subjsame = next.subject().columns().stream().filter(c -> !c.isVirtual())
						.map(c -> tableToMergeInto.name() + "." + c.name() + " = " + next.name() + '.' + c.name())
						.collect(Collectors.joining(" AND "));

				String insert = "INSERT INTO " + tableToMergeInto.name() + "(" + listOfInsert + ") SELECT "
						+ listOfInsert + " FROM " + next.name();

				String update = "UPDATE " + tableToMergeInto.name() + " SET " + toMerge.stream()
						.map(oc -> oc.name() + " = " + next.name() + "." + oc.name()).collect(Collectors.joining(" , "))
						+ " FROM " + next.name() + " WHERE " + subjsame;

				String delete = "DELETE FROM " + next.name() + " USING " + tableToMergeInto.name() + " WHERE "
						+ toMerge.stream().map(
								oc -> next.name() + "." + oc.name() + " = " + tableToMergeInto.name() + "." + oc.name())
								.collect(Collectors.joining(" AND "));

				log.info(update);
				log.info(delete);
				log.info(insert);
				stat.execute(update);
				JdbcUtil.commitIfNeeded(conn);
				stat.execute(delete);
				JdbcUtil.commitIfNeeded(conn);
				stat.execute(insert);
				JdbcUtil.commitIfNeeded(conn);
				tableToMergeInto.objects().add(pm);
			}
		}
	}

	private long size(Table t, Connection conn) {

		try (Statement stat = conn.createStatement()) {
			String dc = "SELECT COUNT(*) AS count FROM " + t.name();
			log.info("Executing " + dc);
			try (ResultSet rs = stat.executeQuery(dc)) {
				while (rs.next()) {
					return rs.getLong(1);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		return 0L;
	}

	private long distinctSubject(Table t, Connection conn, List<Column> physicalColumns) {

		try (Statement stat = conn.createStatement()) {
			String dc = "SELECT COUNT(*) AS count FROM " + t.name() + " GROUP BY "
					+ physicalColumns.stream().map(Column::name).collect(Collectors.joining(","));
			log.info("Executing " + dc);
			try (ResultSet rs = stat.executeQuery(dc)) {
				while (rs.next()) {
					return rs.getLong(1);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		return 0L;
	}

	public List<Column> physicalColumns(Table t) {
		return t.subject().columns().stream().filter(Column::isPhysical).collect(Collectors.toList());
	}
}
