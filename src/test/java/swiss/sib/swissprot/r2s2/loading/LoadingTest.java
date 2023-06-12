package swiss.sib.swissprot.r2s2.loading;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import swiss.sib.swissprot.r2s2.optimization.TableMergingConcurence;
import swiss.sib.swissprot.r2s2.r2rml.R2RMLFromTables;
import swiss.sib.swissprot.r2s2.sql.Table;

public class LoadingTest {

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void simpleTest() throws IOException, SQLException {
		File newFolder = temp.newFile("f");
		File input = writeTestData(newFolder);

		assertDuckDbAvailable();
		Loader loader = new Loader(newFolder, 1);

		final List<String> lines = List.of(input.getAbsolutePath() + "\thttp://example.org/graph");
		List<Table> tables = loader.stepOne(lines, newFolder.getAbsolutePath());
		validateRdfTypeStatementsLoaded(newFolder, tables);
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			R2RMLFromTables.write(tables, out);
			System.out.write(out.toByteArray());
		}
		tables = loader.stepTwo(newFolder.getAbsolutePath(), tables);
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			R2RMLFromTables.write(tables, out);
			System.out.write(out.toByteArray());
		}
		try (Connection conn = open(newFolder)) {
			tables = new TableMergingConcurence(conn, tables).run();
			validateRdfMerged(conn);
		}
	}

	private void validateRdfMerged(Connection conn) throws SQLException {
		try (java.sql.Statement count = conn.createStatement();
				var rs = count.executeQuery("SELECT COUNT(*) FROM type_rdf_Bag")) {
			assertTrue(rs.next());
			assertEquals(2, rs.getInt(1));
			assertFalse(rs.next());
		}

		try (java.sql.Statement count = conn.createStatement();
				var rs = count.executeQuery(
						"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='type_rdf_Alt'")) {
			assertTrue(rs.next());
			assertEquals(rs.getString(1), "subject_rdf_type_parts");
			assertTrue(rs.next());
			assertEquals(rs.getString(1), "object_rdfs_label_langvalue");
			assertFalse(rs.next());
		}

		try (java.sql.Statement count = conn.createStatement();
				var rs = count.executeQuery("SELECT object_rdfs_label_langvalue FROM type_rdf_Alt")) {
			assertTrue(rs.next());
			assertTrue(rs.next());
			assertTrue(rs.next());
			assertFalse(rs.next());
		}

		try (java.sql.Statement count = conn.createStatement();
				var rs = count.executeQuery("SELECT COUNT(object_rdfs_label_langvalue) FROM type_rdf_Alt")) {
			assertTrue(rs.next());
			assertEquals(2, rs.getInt(1));
			assertFalse(rs.next());
		}

	}

	public File writeTestData(File newFolder) throws IOException, FileNotFoundException {
		newFolder.delete();
		SimpleValueFactory vf = SimpleValueFactory.getInstance();

		List<Statement> statements = List.of(vf.createStatement(RDF.BAG, RDF.TYPE, RDF.ALT),
				vf.createStatement(RDF.ALT, RDF.TYPE, RDF.BAG), vf.createStatement(RDF.ALT, RDF.TYPE, RDF.ALT),
				vf.createStatement(RDF.LIST, RDF.TYPE, RDF.ALT), vf.createStatement(RDF.LIST, RDF.TYPE, RDF.BAG),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral(true)),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral(false)),
				vf.createStatement(RDF.LIST, RDFS.LABEL, vf.createLiteral(false)),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral("杭州市", "cz")),
				vf.createStatement(RDF.BAG, RDFS.LABEL, vf.createLiteral("杭州", "cz")),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral("lala", "en-UK")),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral("lala lala", "en-UK")),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral("2023-06-23", XSD.DATE)),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral("2023-06-22", XSD.DATE)),
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
		return input;
	}

	public void assertDuckDbAvailable() {
		try {
			Class.forName("org.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e1) {
			fail("No DuckDB");
		}
	}

	private void validateRdfTypeStatementsLoaded(File newFolder, List<Table> tables) throws SQLException {
		try (Connection conn_rw = open(newFolder);) {
			for (Table t : tables) {
				if (t.objects().get(0).predicate().equals(RDF.TYPE)) {
					try (java.sql.Statement count = conn_rw.createStatement();
							var rs = count.executeQuery("SELECT COUNT(*) FROM " + t.name())) {
						assertTrue(rs.next());
						assertEquals(5, rs.getInt(1));
						assertFalse(rs.next());
					}

					try (java.sql.Statement count = conn_rw.createStatement();
							var rs = count
									.executeQuery("SELECT COUNT(DISTINCT object_rdf_type_parts) FROM " + t.name())) {
						assertTrue(rs.next());
						assertEquals(2, rs.getInt(1));
						assertFalse(rs.next());
					}
				}
			}
		}
	}

	public Connection open(File newFolder) throws SQLException {
		return DriverManager.getConnection("jdbc:duckdb:" + newFolder.getAbsolutePath());
	}
}
