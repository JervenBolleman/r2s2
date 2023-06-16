package swiss.sib.swissprot.r2s2.loading;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import it.unibz.inf.ontop.injection.OntopSQLOWLAPIConfiguration;
import it.unibz.inf.ontop.rdf4j.repository.OntopRepository;

public class SparqlOnLoadedTest {
	@TempDir
	public File temp;

	@Test
	public void loadAndQueryForTypes() throws IOException, SQLException {
		File newFolder = new File(temp, "f");
		File input = new File(temp, "input.rdf");
		File propertyFile = new File(temp, "test.properties");
		LoadingTest.writeTestData(input);
		final List<String> lines = List.of(input.getAbsolutePath() + "\thttp://example.org/graph");
		Loader loader = new Loader(newFolder, 0, lines);
		loader.parse();
		LoadingTest.writeR2RML(loader.tables());
		try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + newFolder.getAbsolutePath());
				var statement = conn.createStatement()) {
			try (ResultSet rs = statement
					.executeQuery("SELECT table_name, column_name,data_type FROM information_schema.columns")) {
				while (rs.next()) {
					System.out.println(rs.getString(1) + '.' + rs.getString(2) +" : "+ rs.getString(3));
				}
			}

		}
		Files.writeString(propertyFile.toPath(), "jdbc.url = jdbc:duckdb:" + newFolder.getAbsolutePath());
		OntopSQLOWLAPIConfiguration configuration = OntopSQLOWLAPIConfiguration.defaultBuilder()
				.r2rmlMappingFile(loader.r2rmlPath()).propertyFile(propertyFile).enableTestMode().build();

		Repository repo = OntopRepository.defaultRepository(configuration);
		repo.init();

		try (RepositoryConnection conn = repo.getConnection();
				TupleQueryResult result = conn
						.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT DISINCT ?type WHERE {[] a ?type}")
						.evaluate()) {
			while (result.hasNext()) {
				BindingSet bindingSet = result.next();
				System.out.println(bindingSet);
			}
		}
	}
}
