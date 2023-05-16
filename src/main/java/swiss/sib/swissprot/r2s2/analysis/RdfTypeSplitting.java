package swiss.sib.swissprot.r2s2.analysis;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

	public List<Table> split(Connection conn, List<Table> tables) {
		List<Table> newTables = new ArrayList<>();
		for (Iterator<Table> iterator = tables.iterator(); iterator.hasNext();) {
			Table t = iterator.next();
			if (t.objects().size() == 1) {
				PredicateMap pm = t.objects().get(0);
				if (pm.predicate().equals(RDF.TYPE)) {
					newTables.addAll(split(t, iterator, conn, pm));
					break;
				}
			}
		}
		newTables.addAll(tables);
		return newTables;
	}

	private List<Table> split(Table t, Iterator<Table> iterator, Connection conn, PredicateMap pm) {
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
				String columns = notVirtual.stream().map(Column::name).collect(Collectors.joining(","));
				String dc = "SELECT DISTINCT " + columns + " FROM " + t.name();
				log.info("Executing " + dc);
				try (ResultSet rs = stat.executeQuery(dc)) {
					while (rs.next()) {
						Table newTable = new Table(pm.predicate(), new Columns(t.subject().getColumns()),
								t.subjectKind(), new Columns(pm.columns().getColumns()), pm.objectKind(), t.graph(),
								pm.lang(), pm.datatype());
						newTables.add(newTable);
						newTable.create(conn);
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
						}
					}
				}

				iterator.remove();
				try (Statement update = conn.createStatement()) {
					String drop = "DROP TABLE " + t.name();
					log.info("Executing" + drop);
					update.execute(drop);
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return newTables;
	}
}
