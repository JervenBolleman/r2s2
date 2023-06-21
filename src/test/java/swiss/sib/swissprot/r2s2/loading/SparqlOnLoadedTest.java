package swiss.sib.swissprot.r2s2.loading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static swiss.sib.swissprot.r2s2.JdbcUtil.openByJdbc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.unibz.inf.ontop.injection.OntopSQLOWLAPIConfiguration;
import it.unibz.inf.ontop.rdf4j.repository.OntopRepository;

public class SparqlOnLoadedTest {

    static Stream<Arguments> driverStrings(){
        return Stream.of(
//        		Arguments.of("jdbc:duckdb:", "org.duckdb.DuckDBDriver", "f.main"),
                Arguments.of("jdbc:h2:file:","org.h2.Driver",""));
    }

	@TempDir
	public File temp;
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final String NS = "https://example.org/";

	
	public String jdbc;
	
	@Disabled
	@ParameterizedTest
	@MethodSource("driverStrings")
	public void loadAndQueryForTypes(String jdbcUrlPrefix, String jdbcDriver, String dbName) throws Exception {
		List<Statement> statements = LoadingTest.statements;
		testTypePresence(statements, jdbcUrlPrefix, jdbcDriver, 2, dbName);
	}

	@ParameterizedTest
	@MethodSource("driverStrings")
	public void loadAndQueryForTypes2(String jdbcUrlPrefix, String jdbcDriver, String dbName) {
		List<Statement> statements = IntStream.range(1, 100).mapToObj(i -> VF.createStatement(VF.createIRI(NS, "i" + i),
				RDF.TYPE, VF.createIRI(NS, i % 2 == 0 ? "odd" : "even"))).collect(Collectors.toList());

		try {
			testTypePresence(statements, jdbcUrlPrefix, jdbcDriver, 2, dbName);
		} catch (Exception e) {
			fail(e.getMessage(), e);
		}
	}

	@ParameterizedTest
	@MethodSource("driverStrings")
	public void loadAndQueryForTypes3(String jdbcUrlPrefix, String jdbcDriver, String dbName) {
		List<Statement> statements = IntStream.range(1, 100).mapToObj(i -> VF.createStatement(VF.createIRI(NS, "i" + i),
				RDF.TYPE, VF.createIRI(NS, i % 2 == 0 ? "odd" : "even"))).collect(Collectors.toList());
		IntStream.range(1, 100)
				.mapToObj(i -> VF.createStatement(VF.createIRI(NS, "i" + i), RDFS.LABEL, VF.createLiteral(i)))
				.forEach(statements::add);
		try {
			testTypePresence(statements,jdbcUrlPrefix, jdbcDriver, 2, dbName);
		} catch (Exception e) {
			fail(e.getMessage(), e);
		}
	}

	private void testTypePresence(List<Statement> statements, String jdbcUrlPrefix,String jdbcDriver, int types, String dbName)
			throws IOException, FileNotFoundException, SQLException, Exception {
		File newFolder = new File(temp, "f");
		File input = new File(temp, "input.rdf");
		File propertyFile = new File(temp, "test.properties");
		LoadingTest.writeTestData(input, statements);
		final List<String> lines = List.of(input.getAbsolutePath() + "\thttp://example.org/graph");
		final String jdbc = jdbcUrlPrefix + newFolder.getAbsolutePath();
		Loader loader = new Loader(newFolder, 0, lines, jdbc);
		loader.parse();
//		LoadingTest.writeR2RML(loader.tables());
		
		Files.writeString(propertyFile.toPath(), "jdbc.url=" + jdbc +"\njdbc.Driver="+jdbcDriver+"\njdbc.name="+dbName);
		OntopSQLOWLAPIConfiguration configuration = OntopSQLOWLAPIConfiguration.defaultBuilder()
				.r2rmlMappingFile(loader.r2rmlPath()).propertyFile(propertyFile).enableTestMode().build();
		testSparqlDistinctTypes(types, configuration);		
		testSqlCountOfTypes(types, jdbc);
	}

	public void testSparqlDistinctTypes(int types, OntopSQLOWLAPIConfiguration configuration) throws Exception {
		try (OntopRepository repo = OntopRepository.defaultRepository(configuration)) {
			repo.init();

			try (RepositoryConnection conn = repo.getConnection();
					TupleQueryResult result = conn
							.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT DISTINCT ?type FROM <http://example.org/graph> WHERE {[] a ?type}")
							.evaluate()) {
				for (int i = 0; i < types; i++) {
					assertTrue(result.hasNext(), " we should have " + types + " types");
					BindingSet bindingSet = result.next();
					assertNotNull(bindingSet);
				}
				assertFalse(result.hasNext(), " we should have " + types + " types");
			}
		}
	}

	public void testSqlCountOfTypes(int types, final String jdbc) throws SQLException {
//		try (Connection conn = openByJdbc(jdbc); var statement = conn.createStatement()) {
//			try (ResultSet rs = statement.executeQuery(
//					"SELECT table_name FROM information_schema.tables")){// WHERE UCASE(table_name) LIKE 'TYPE%'")) {
//				while(rs.next()) {
//					System.out.println(rs.getString(1));
//				}
//			}
//		}
		try (Connection conn = openByJdbc(jdbc); var statement = conn.createStatement()) {
			try (ResultSet rs = statement.executeQuery(
					"SELECT COUNT(table_name) FROM information_schema.tables WHERE UCASE(table_name) LIKE 'TYPE%'")) {
				assertTrue(rs.next());
				assertEquals(types, rs.getInt(1));
			}
		}
	}
}
