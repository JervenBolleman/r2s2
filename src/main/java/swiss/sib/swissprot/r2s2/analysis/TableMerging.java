package swiss.sib.swissprot.r2s2.analysis;

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

import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;

public class TableMerging {
	private static final Logger log = LoggerFactory.getLogger(TableMerging.class);

	public TableMerging() {
	}

	public List<Table> merge(Connection conn, List<Table> tables) throws SQLException {
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
			}
		}
		Map<List<Column>, List<Table>> collect = mergeCandidates.stream()
				.collect(Collectors.groupingBy((t) -> t.subject().getColumns()));
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
				notMerged(notMerged, tablesToMerge);
			}
		}
		notMerged.addAll(merged);
		return notMerged;
	}

	private void notMerged(List<Table> notMerged, List<Table> tablesToMerge) {
		var tablesToMergeIter = tablesToMerge.iterator();
		assert tablesToMergeIter.hasNext();
		notMerged.add(tablesToMergeIter.next());
		assert !tablesToMergeIter.hasNext();
	}

	private void merge(Table tableToMergeInto, Table next, Connection conn) throws SQLException {
		log.info("Merging " + next.name() + " into " + tableToMergeInto.name());
		try (var stat = conn.createStatement()) {
			for (PredicateMap pm : next.objects()) {
				List<Column> toMerge = new ArrayList<>();
				for (Column oc : pm.columns().getColumns()) {
					if (oc.isPhysical()) {
						String alter = "ALTER TABLE " + tableToMergeInto.name() + " ADD COLUMN " + oc.definition();
						log.info(alter);
						stat.execute(alter);
						toMerge.add(oc);
					}
				}

				String listOfInsert = Stream.concat(next.subject().getColumns().stream(), toMerge.stream())
						.filter(c -> !c.isVirtual()).map(c -> c.name()).collect(Collectors.joining(", "));

				String subjsame = next.subject().getColumns().stream().filter(c -> !c.isVirtual())
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
				stat.execute(delete);
				stat.execute(insert);
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
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
		return t.subject().getColumns().stream().filter(Column::isPhysical).collect(Collectors.toList());
	}
}
