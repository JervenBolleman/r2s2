package swiss.sib.swissprot.r2s2.optimization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;

import swiss.sib.swissprot.r2s2.loading.LoadIntoTable;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap.TempIriId;
import swiss.sib.swissprot.r2s2.sql.Table;

public class ReintroduceRdfSetSemanticsTest {
	private static final String NS = "http://example.org/";
	private static final ValueFactory vf = SimpleValueFactory.getInstance();
	private static final IRI zeroIri = vf.createIRI(NS, "1");
	private static final IRI oneIri = vf.createIRI(NS, "2");
	private static final IRI zeroGraph = vf.createIRI(NS, "zeroGraph");
	private static final IRI oneGraph = vf.createIRI(NS, "oneGraph");

	@Test
	void twoGraphsOneSubject() throws SQLException, IOException {
		Map<String, String> ns = Map.of(RDF.PREFIX, RDF.NAMESPACE, "ex", NS);
		TemporaryIriIdMap p = new TemporaryIriIdMap();
		TempIriId pt = p.temporaryIriId(RDF.TYPE);
		try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {

			Statement test = vf.createStatement(oneIri, pt, zeroIri, zeroGraph);
			Statement test2 = vf.createStatement(oneIri, pt, zeroIri, oneGraph);

			Table t = load(ns, conn, test, test2, p, pt);
			assertEquals(2, countAllRows(t.name(), conn));
		}
	}

	private long countAllRows(String name, Connection conn) throws SQLException {
		try (var s = conn.createStatement(); var rs = s.executeQuery("SELECT COUNT(*) FROM " + name)) {
			assertTrue(rs.next());
			return rs.getLong(1);
		}
	}

	private Table load(Map<String, String> ns, Connection conn, Statement test, Statement test2, TemporaryIriIdMap p,
			TempIriId pt) throws SQLException, IOException {
		try (LoadIntoTable loadIntoTable = new LoadIntoTable(test, conn, p, pt, ns)) {
			assertTrue(loadIntoTable.testForAcceptance(test));
			loadIntoTable.write(test);
			assertTrue(loadIntoTable.testForAcceptance(test2));
			loadIntoTable.write(test2);
			return loadIntoTable.table();
		}
	}
}
