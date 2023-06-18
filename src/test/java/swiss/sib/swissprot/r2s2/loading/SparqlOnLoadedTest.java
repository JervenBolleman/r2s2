package swiss.sib.swissprot.r2s2.loading;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import it.unibz.inf.ontop.injection.OntopSQLOWLAPIConfiguration;
import it.unibz.inf.ontop.rdf4j.repository.OntopRepository;

public class SparqlOnLoadedTest {
	@TempDir
	public File temp;
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final String NS = "https://example.org/";

	@Disabled
	@Test
	public void loadAndQueryForTypes() throws Exception {
		List<Statement> statements = LoadingTest.statements;
		testTypePresence(statements);
	}

	@Disabled
	@Test
	public void loadAndQueryForTypes2() {
		List<Statement> statements = IntStream.range(1, 100).mapToObj(i -> VF.createStatement(VF.createIRI(NS, "i" + i),
				RDF.TYPE, VF.createIRI(NS, i % 2 == 0 ? "odd" : "even"))).collect(Collectors.toList());

		try {
			testTypePresence(statements);
		} catch (Exception e) {
			fail(e);
		}
	}

	@Disabled
	@Test
	public void loadAndQueryForTypes3() throws Exception {
		List<Statement> statements = IntStream.range(1, 100).mapToObj(i -> VF.createStatement(VF.createIRI(NS, "i" + i),
				RDF.TYPE, VF.createIRI(NS, i % 2 == 0 ? "odd" : "even"))).collect(Collectors.toList());
		IntStream.range(1, 100)
				.mapToObj(i -> VF.createStatement(VF.createIRI(NS, "i" + i), RDFS.LABEL, VF.createLiteral(i)))
				.forEach(statements::add);
		try {
			testTypePresence(statements);
		} catch (Exception e) {
			fail(e);
		}
	}

	private void testTypePresence(List<Statement> statements)
			throws IOException, FileNotFoundException, SQLException, Exception {
		File newFolder = new File(temp, "f");
		File input = new File(temp, "input.rdf");
		File propertyFile = new File(temp, "test.properties");
		LoadingTest.writeTestData(input, statements);
		final List<String> lines = List.of(input.getAbsolutePath() + "\thttp://example.org/graph");
		Loader loader = new Loader(newFolder, 0, lines);
		loader.parse();
		LoadingTest.writeR2RML(loader.tables());
		try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + newFolder.getAbsolutePath());
				var statement = conn.createStatement()) {
			try (ResultSet rs = statement
					.executeQuery("SELECT table_name, column_name,data_type FROM information_schema.columns")) {
				while (rs.next()) {
					System.out.println(rs.getString(1) + '.' + rs.getString(2) + " : " + rs.getString(3));
				}
			}
			
			try (ResultSet rs = statement
					.executeQuery("SELECT * FROM type_rdf_Bag")) {
				while (rs.next()) {
					System.out.println(rs.getString(1) + '\t' + rs.getString(2));
				}
			}

		}
		Files.writeString(propertyFile.toPath(), "jdbc.url = jdbc:duckdb:" + newFolder.getAbsolutePath());
		OntopSQLOWLAPIConfiguration configuration = OntopSQLOWLAPIConfiguration.defaultBuilder()
				.r2rmlMappingFile(loader.r2rmlPath()).propertyFile(propertyFile).enableTestMode().build();

		try (OntopRepository repo = OntopRepository.defaultRepository(configuration)) {
			repo.init();

			try (RepositoryConnection conn = repo.getConnection();
					TupleQueryResult result = conn
							.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT DISTINCT ?type WHERE {[] a ?type}")
							.evaluate()) {
				assertTrue(result.hasNext());
				while (result.hasNext()) {
					BindingSet bindingSet = result.next();
					System.out.println(bindingSet);
				}
			}
		}
	}
}
