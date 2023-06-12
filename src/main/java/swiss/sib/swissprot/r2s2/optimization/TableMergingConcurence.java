package swiss.sib.swissprot.r2s2.optimization;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.rdf4j.model.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.DuckDBUtil;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;
import swiss.sib.swissprot.r2s2.sql.VirtualSingleValueColumn;

public class TableMergingConcurence {
	private static final Logger logger = LoggerFactory.getLogger(TableMergingConcurence.class);
	private final Connection conn;
	private final List<Table> tables;

	public TableMergingConcurence(Connection conn, List<Table> tables) {
		this.conn = conn;
		this.tables = new ArrayList<>(tables);
	}

	public List<Table> run() throws SQLException {
		List<Table> mergeCandidates = tables.stream().filter(this::hasDistinctSubjects).collect(Collectors.toList());
		Iterator<Table> mergeCandidatesIter = mergeCandidates.iterator();
		while (mergeCandidatesIter.hasNext()) {
			Table mergeCandidate = mergeCandidatesIter.next();
			Iterator<Table> overlappingSubjects = findTablesWithOverlappingSubjects(mergeCandidate,
					List.copyOf(mergeCandidates));
			while (overlappingSubjects.hasNext()) {
				merge(mergeCandidate, overlappingSubjects.next());
			}
		}
		removeEmpty(tables);
		return tables;
	}

	private void removeEmpty(List<Table> tables) {
		Iterator<Table> ti = tables.iterator();
		while (ti.hasNext()) {
			Table t = ti.next();
			String sql = "SELECT * FROM " + t.name() + " LIMIT 1";
			logger.info("Running " + sql);
			boolean empty = false;
			try (var stat = conn.createStatement(); ResultSet rs = stat.executeQuery(sql)) {
				if (!rs.next()) {
					empty = true;
				}
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
			if (empty) {
				try (var stat = conn.createStatement()) {
					String dropSql = "DROP TABLE " + t.name();
					logger.info("Running " + sql);
					stat.execute(dropSql);
					ti.remove();
				} catch (SQLException e) {
					throw new IllegalStateException(e);
				}
			}
		}

	}

	private void merge(Table mc, Table other) throws SQLException {
		logger.info("Merging " + other.name() + " into " + mc.name());
		try (var stat = conn.createStatement()) {
			for (PredicateMap pm : other.objects()) {
				List<Column> toMerge = new ArrayList<>();
				for (Column oc : pm.columns().getColumns()) {
					if (oc.isPhysical()) {
						String alter = "ALTER TABLE " + mc.name() + " ADD COLUMN " + oc.definition();
						logger.info(alter);
						stat.execute(alter);
						toMerge.add(oc);
					}
				}

				String msc = concatColumns(mc, "mc");
				String osc = concatColumns(other, "oc");

				String update = "UPDATE " + mc.name() + " SET "
						+ toMerge.stream().map(oc -> "" + oc.name() + " = oc." + oc.name())
								.collect(Collectors.joining(" , "))
						+ " FROM " + other.name() + " oc WHERE " + concatColumns(mc, "") + '=' + osc;

				String delete = "DELETE FROM " + other.name() + " oc USING " + mc.name() + " mc WHERE " + msc + '='
						+ osc;

				logger.info(update);
				stat.execute(update);
				DuckDBUtil.commitIfNeeded(conn);
				logger.info(delete);
				stat.execute(delete);
				DuckDBUtil.commitIfNeeded(conn);
				mc.objects().add(pm);
			}
		}
	}

	private Iterator<Table> findTablesWithOverlappingSubjects(Table mc, List<Table> tables) {
		List<Table> compatible = new ArrayList<>();
		for (Table other : tables) {
			final boolean sameSubjectKind = mc.subjectKind().equals(other.subjectKind());
			Set<IRI> mcPredicates = mc.objects().stream().map(PredicateMap::predicate).collect(Collectors.toSet());
			Set<IRI> otherPredicates = other.objects().stream().map(PredicateMap::predicate)
					.collect(Collectors.toSet());
			final Stream<IRI> filter = mcPredicates.stream().filter(otherPredicates::contains);
			final boolean samePredicate = filter.findAny().isPresent();
			if (other != mc && sameSubjectKind && !samePredicate) {
				try (Statement statement = conn.createStatement()) {
					String msc = concatColumns(mc, "mc");
					String osc = concatColumns(other, "oc");
					String sql = "SELECT COUNT(*) FROM " + mc.name() + " mc , " + other.name() + " oc WHERE " + msc
							+ '=' + osc;
					logger.info("Running " + sql);
					try (ResultSet rs = statement.executeQuery(sql)) {
						rs.next();
						final boolean isAMergeCandidate = rs.getInt(1) > 0;
						if (isAMergeCandidate) {
							logger.info(mc.name() + " is a merge candidate with " + other.name() + " overlapping ");
							compatible.add(other);
						} else {
							logger.info(mc.name() + " is NOT a merge candidate with " + other.name());
						}
					}
				} catch (SQLException e) {
					throw new IllegalStateException(e);
				}
			}
		}
		return compatible.iterator();
	}

	public String concatColumns(Table mc, String alias) {
		return mc.subject().getColumns().stream().map(c -> {
			if (c.isPhysical()) {
				if (alias.isEmpty()) {
					return c.name();
				} else {
					return alias + '.' + c.name();
				}
			} else {
				return "'" + ((VirtualSingleValueColumn) c).value() + "'";
			}
		}).collect(Collectors.joining("||"));
	}

	private boolean hasDistinctSubjects(Table t) {
		String sc = t.subject().getColumns().stream().filter(Column::isPhysical).map(Column::name)
				.collect(Collectors.joining(","));
		if (sc.isEmpty()) {
			return false;
		}
		try (Statement statement = conn.createStatement()) {
			String sql = "SELECT " + sc + " FROM " + t.name() + " GROUP BY " + sc + " HAVING (COUNT(" + sc
					+ ") > 1) LIMIT 1";
			logger.info("Running " + sql);
			try (ResultSet rs = statement.executeQuery(sql)) {
				final boolean isNotAMergeCandidate = rs.next();
				if (isNotAMergeCandidate) {
					logger.info(t.name() + " is NOT a merge candidate");
				} else {
					logger.info(t.name() + " is a merge candidate");
				}
				return !isNotAMergeCandidate;
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}
}
