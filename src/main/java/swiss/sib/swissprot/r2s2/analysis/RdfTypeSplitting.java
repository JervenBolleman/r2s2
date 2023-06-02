package swiss.sib.swissprot.r2s2.analysis;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.Columns;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;

public class RdfTypeSplitting {
	private static final Logger log = LoggerFactory.getLogger(RdfTypeSplitting.class);

	public RdfTypeSplitting() {

	}

	private static final AtomicInteger TYPE_ID = new AtomicInteger(0);

	public List<Table> split(Connection conn, List<Table> tables, Map<String, String> namespaces) {
		List<Table> newTables = new ArrayList<>();
		for (Iterator<Table> iterator = tables.iterator(); iterator.hasNext();) {
			Table t = iterator.next();
			if (t.objects().size() == 1) {
				PredicateMap pm = t.objects().get(0);
				if (pm.predicate().equals(RDF.TYPE)) {
					newTables.addAll(split(t, iterator, conn, pm, namespaces));
					break;
				}
			}
		}
		newTables.addAll(tables);
		return newTables;
	}

	private List<Table> split(Table t, Iterator<Table> iterator, Connection conn, PredicateMap pm,
			Map<String, String> namespaces) {
		List<Table> newTables = new ArrayList<>();
		List<Column> notVirtual = new ArrayList<>();
		List<Column> virtual = new ArrayList<>();
		for (Column c : pm.columns().getColumns()) {
			if (c.isVirtual())
				virtual.add(c);
			else
				notVirtual.add(c);
		}
		if (notVirtual.isEmpty()) {
			log.info("Nothing to be done for class cracking");
		} else {
			try (Statement stat = conn.createStatement()) {
				String columns = notVirtual.stream().map(Column::name).collect(Collectors.joining(", "));

				String dc = "SELECT DISTINCT " + columns + " FROM " + t.name();
				log.info("Executing " + dc);
				try (ResultSet rs = stat.executeQuery(dc)) {
					while (rs.next()) {
						Table newTable;
						try {
							String tableName = newTableName(notVirtual, rs, namespaces);
							newTable = makeNewTable(t, conn, pm, tableName);
						} catch (SQLException e) {
							//Can happen if the table name is not valid.
							newTable = new Table("type_" + TYPE_ID.incrementAndGet(), new Columns(t.subject().getColumns()),
									t.subjectKind(), List.of(pm), t.graph());
						}
						newTables.add(newTable);
						String in = "INSERT INTO " + newTable.name() + " (SELECT * FROM " + t.name() + " WHERE ";
						for (int j = 0; j < notVirtual.size(); j++) {
							Column c = notVirtual.get(j);
							in += c.name() + " = '" + rs.getString(j + 1) + "'";
							if (j != notVirtual.size() - 1) {
								in += " AND ";
							}
						}
						in += ")";
						log.info("Executing " + in);
						try (Statement update = conn.createStatement()) {
							update.executeUpdate(in);
							if (!conn.getAutoCommit()) {
								conn.commit();
							}
						}
					}
				}

				iterator.remove();
				try (Statement update = conn.createStatement()) {
					String drop = "DROP TABLE " + t.name();
					log.info("Executing" + drop);
					update.execute(drop);
					if (!conn.getAutoCommit()) {
						conn.commit();
					}
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return newTables;
	}

	private Table makeNewTable(Table t, Connection conn, PredicateMap pm, String tableName) throws SQLException {
		Table newTable = new Table("type_" + tableName, new Columns(t.subject().getColumns()),
				t.subjectKind(), List.of(pm), t.graph());
		newTable.create(conn);
		return newTable;
	}

	private String newTableName(List<Column> notVirtual, ResultSet rs, Map<String, String> namespaces)
			throws SQLException {
		String typeIri = rs.getObject(1).toString();
		for (int i = 2; i <= notVirtual.size(); i++) {
			typeIri += rs.getObject(i).toString();
		}
		for (Map.Entry<String, String> en : namespaces.entrySet()) {
			if (typeIri.startsWith(en.getValue()))
				return en.getKey() + "_" + typeIri.substring(en.getValue().length());
		}
		return "type_" + TYPE_ID.incrementAndGet();
	}
}
