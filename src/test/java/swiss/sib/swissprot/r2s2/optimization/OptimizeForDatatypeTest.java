package swiss.sib.swissprot.r2s2.optimization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.Test;

import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.GroupOfColumns;
import swiss.sib.swissprot.r2s2.sql.SqlDatatype;

public class OptimizeForDatatypeTest {

	@Test
	public void allNumberQuery() throws SQLException {
		String cn = "object_iri" + GroupOfColumns.PATH;
		Column c = new Column(cn, SqlDatatype.TEXT);
		try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
			try (var ct = conn.createStatement()) {
				ct.execute("CREATE TABLE t(" + c.definition() + ")");
			}
			for (int i = 0; i < 100; i++) {
				addToTestTable(conn, Integer.toString(i));
			}
			OptimizeForDatatype.optimizeIRI(conn, "t", List.of(c));
			assertEquals(c.sqlDatatype(), SqlDatatype.INTEGER);
		}
	}

	@Test
	public void allNumberQueryButWithLeadingZeros() throws SQLException {
		String cn = "object_iri" + GroupOfColumns.PATH;
		Column c = new Column(cn, SqlDatatype.TEXT);
		try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
			try (var ct = conn.createStatement()) {
				ct.execute("CREATE TABLE t(" + c.definition() + ")");
			}
			for (int i = 0; i < 100; i++) {
				addToTestTable(conn, "0" + Integer.toString(i));
			}
			OptimizeForDatatype.optimizeIRI(conn, "t", List.of(c));
			assertEquals(c.sqlDatatype(), SqlDatatype.TEXT);
		}
	}
	
	@Test
	public void allSomeNumberQuery() throws SQLException {
		String cn = "object_iri" + GroupOfColumns.PATH;
		Column c = new Column(cn, SqlDatatype.TEXT);
		try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
			try (var ct = conn.createStatement()) {
				ct.execute("CREATE TABLE t(" + c.definition() + ")");
			}
			for (int i = 0; i < 100; i++) {
				addToTestTable(conn, Integer.toString(i));
			}
			addToTestTable(conn, "A");
			OptimizeForDatatype.optimizeIRI(conn, "t", List.of(c));
			assertEquals(c.sqlDatatype(), SqlDatatype.TEXT);
		}
	}

	private void addToTestTable(Connection conn, String zero) throws SQLException {
		try (PreparedStatement prepareStatement = conn.prepareStatement("INSERT INTO t VALUES (?)")) {
			prepareStatement.setString(1, zero);
			prepareStatement.execute();
		}
	}
}
