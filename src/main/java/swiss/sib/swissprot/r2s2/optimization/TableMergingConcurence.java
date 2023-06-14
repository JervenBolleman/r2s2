package swiss.sib.swissprot.r2s2.optimization;

import static swiss.sib.swissprot.r2s2.DuckDBUtil.open;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
	private final String path;
	private final List<Table> tables;
	private final Set<Table> mergedTables = new HashSet<>();
	private final Map<Table, Long> sizes = new HashMap<>();

	public TableMergingConcurence(String tempPath, List<Table> tables) {
		this.path = tempPath;
		this.tables = new ArrayList<>(tables);
		this.tables.sort((a, b) -> compareTablesFirstTypeThenSize(a, b));
	}

	public List<Table> run() {
		logger.info("Starting merging tables");
		List<Table> mergeCandidates = tables.stream().filter(this::hasDistinctSubjects)
				.sorted((a, b) -> compareTablesFirstTypeThenSize(a, b)).collect(Collectors.toList());
		Iterator<Table> mergeCandidatesIter = mergeCandidates.iterator();
		while (mergeCandidatesIter.hasNext()) {
			Table mergeCandidate = mergeCandidatesIter.next();
			if (!isTableEmpty(mergeCandidate)) {
				Iterator<Table> overlappingSubjects = findTablesWithOverlappingSubjects(mergeCandidate,
						List.copyOf(mergeCandidates));
				while (overlappingSubjects.hasNext()) {
					final Table other = overlappingSubjects.next();
					merge(mergeCandidate, other);
					if (isTableEmpty(other)) {
						overlappingSubjects.remove();
					}
				}
			} else {
				mergeCandidatesIter.remove();
			}
			mergedTables.add(mergeCandidate);
		}
		removeEmpty(tables);
		logger.info("Finished merging tables");
		return tables;
	}

	/**
	 * We want to merge into small tables to generate denser columns.
	 * 
	 * @param a table
	 * @param b table
	 * @return a type column first, then the smallest
	 */
	private int compareTablesFirstTypeThenSize(Table a, Table b) {
		if (a.name().startsWith("type_") && !b.name().startsWith("type_"))
			return -1;
		else if (!a.name().startsWith("type_") && !a.name().startsWith("type_"))
			return 1;
		else {
			return Long.compare(size(a, false), size(b, false));
		}
	}

	private void removeEmpty(List<Table> tables) {
		Iterator<Table> ti = tables.iterator();
		while (ti.hasNext()) {
			Table t = ti.next();
			if (isTableEmpty(t)) {
				try (Connection conn = open(path); var stat = conn.createStatement()) {
					String dropSql = "DROP TABLE " + t.name();
					logger.info("Running " + dropSql);
					stat.execute(dropSql);
					ti.remove();
				} catch (SQLException e) {
					throw new IllegalStateException(e);
				}
			}
		}
	}

	public boolean isTableEmpty(Table t) {
		return size(t, false) == 0;
	}

	private long size(Table t, boolean recalculate) {
		if (recalculate || !sizes.containsKey(t)) {
			String sql = "SELECT COUNT(*) FROM " + t.name() + "";
			logger.info("Running " + sql);
			long size = 0;
			try (Connection conn = open(path);
					var stat = conn.createStatement();
					ResultSet rs = stat.executeQuery(sql)) {
				rs.next();
				size = rs.getLong(1);
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
			sizes.put(t, size);
			return size;
		} else {
			return sizes.get(t);
		}
	}

	private void merge(Table mc, Table other) {
		logger.info("Merging " + other.name() + " into " + mc.name());
		for (PredicateMap pm : other.objects()) {
			List<Column> toMerge = new ArrayList<>();
			try (Connection conn = open(path); var stat = conn.createStatement()) {
				for (Column oc : pm.columns().getColumns()) {
					if (oc.isPhysical()) {
						String alter = "ALTER TABLE " + mc.name() + " ADD COLUMN " + oc.definition();
						logger.info(alter);
						stat.execute(alter);
						toMerge.add(oc);
					}
				}
				if (!toMerge.isEmpty()) {
					merge(mc, other, pm, toMerge, conn, stat);
				}
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	public void merge(Table mc, Table other, PredicateMap pm, List<Column> toMerge, Connection conn, Statement stat)
			throws SQLException {
		String msc = concatColumns(mc, "mc");
		String osc = concatColumns(other, "oc");

		String update = "UPDATE " + mc.name() + " SET "
				+ toMerge.stream().map(oc -> "" + oc.name() + " = oc." + oc.name()).collect(Collectors.joining(" , "))
				+ " FROM " + other.name() + " oc WHERE " + concatColumns(mc, "") + '=' + osc;

		String delete = "DELETE FROM " + other.name() + " oc USING " + mc.name() + " mc WHERE " + msc + '=' + osc;

		logger.info(update);
		stat.execute(update);
		DuckDBUtil.commitIfNeeded(conn);
		logger.info(delete);
		stat.execute(delete);
		DuckDBUtil.commitIfNeeded(conn);
		mc.objects().add(pm);
		size(other, true);
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
			if (other != mc && sameSubjectKind && !samePredicate && !mergedTables.contains(other)
					&& !isTableEmpty(other)) {
				try (Connection conn = open(path); Statement statement = conn.createStatement()) {
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
		try (Connection conn = open(path); Statement statement = conn.createStatement()) {
			String sql = "SELECT " + sc + " FROM " + t.name() + " GROUP BY " + sc + " HAVING (COUNT(DISTINCT(" + sc
					+ ")) > 1) LIMIT 1";
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