package swiss.sib.swissprot.r2s2.loading;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.duckdb.DuckDBDatabaseMetaData;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import swiss.sib.swissprot.r2s2.analysis.IntroduceVirtualColumns;
import swiss.sib.swissprot.r2s2.analysis.RdfTypeSplitting;
import swiss.sib.swissprot.r2s2.analysis.TableMerging;
import swiss.sib.swissprot.r2s2.r2rml.R2RMLFromTables;
import swiss.sib.swissprot.r2s2.sql.Table;

public class LoadingTest {

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void simpleTest() throws IOException, SQLException {
		File newFolder = temp.newFile("f");
		newFolder.delete();
		SimpleValueFactory vf = SimpleValueFactory.getInstance();

		List<Statement> statements = List.of(vf.createStatement(RDF.BAG, RDF.TYPE, RDF.ALT),
				vf.createStatement(RDF.ALT, RDF.TYPE, RDF.BAG), vf.createStatement(RDF.ALT, RDF.TYPE, RDF.ALT),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral(true)),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral(false)),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral("杭州市", "cz")),
				vf.createStatement(RDF.BAG, RDFS.LABEL, vf.createLiteral("杭州", "cz")),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral("lala", "en-UK")),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral("lala lala", "en-UK")),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createBNode("1")));
		Optional<RDFWriterFactory> optional = RDFWriterRegistry.getInstance().get(RDFFormat.RDFXML);
		File input = temp.newFile("input.rdf");
		if (optional.isEmpty())
			fail("Test config error");
		else {
			try (FileOutputStream out = new FileOutputStream(input)) {
				RDFWriter writer = optional.get().getWriter(out);
				writer.startRDF();
				for (Statement st : statements)
					writer.handleStatement(st);
				writer.endRDF();
			}
		}

		Properties p = new Properties();
		try {
			Class.forName("org.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
//		DuckDBDatabaseMetaData db = new DuckDBDatabase("jdbc:duckdb:" + newFolder.getAbsolutePath(), false, p);
		
		Loader loader = Loader.parse(newFolder, List.of(input.getAbsolutePath() + "\thttp://example.org/graph"), 1);
		
		List<Table> tables = loader.tables();
		validateRdfTypeStatementsLoaded(newFolder, tables);
		R2RMLFromTables.write(tables, System.out);

		try (Connection conn_rw = DriverManager.getConnection("jdbc:duckdb:" + newFolder.getAbsolutePath());) {
			tables = new TableMerging().merge(conn_rw, tables);
			RdfTypeSplitting rdfTypeSplitting = new RdfTypeSplitting();
			tables = rdfTypeSplitting.split(conn_rw, tables);
			for (Table table : tables) {
				IntroduceVirtualColumns.optimizeForR2RML( conn_rw, table);
			}
		}
		R2RMLFromTables.write(tables, System.out);
//		db.shutdown();
	}

	private void validateRdfTypeStatementsLoaded(File newFolder, List<Table> tables) throws SQLException {
		try (Connection conn_rw = DriverManager.getConnection("jdbc:duckdb:" + newFolder.getAbsolutePath());) {
			for (Table t : tables) {
				if (t.objects().get(0).predicate().equals(RDF.TYPE)) {
					try (java.sql.Statement count = conn_rw.createStatement();
							var rs = count.executeQuery("SELECT COUNT(*) FROM " + t.name())) {
						assertTrue(rs.next());
						assertEquals(3, rs.getInt(1));
						assertFalse(rs.next());
					}

					try (java.sql.Statement count = conn_rw.createStatement();
							var rs = count.executeQuery("SELECT COUNT(DISTINCT object_0_parts) FROM " + t.name())) {
						assertTrue(rs.next());
						assertEquals(2, rs.getInt(1));
						assertFalse(rs.next());
					}
				}
			}
		}
	}
}
