package swiss.sib.swissprot.r2s2.analysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.Columns;
import swiss.sib.swissprot.r2s2.sql.Datatypes;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;

public class OptimizeForDatatype {
	private static final Logger logger = LoggerFactory.getLogger(OptimizeForDatatype.class);

	public static void optimizeForR2RML(Connection conn, Table table) throws SQLException {
		for (PredicateMap p : table.objects()) {
			for (Column c : p.columns().getColumns()) {
				alterForDatatype(conn, table, p, c, XSD.INT, Datatypes.BIGINT, "");
				alterForDatatype(conn, table, p, c, XSD.LONG, Datatypes.BIGINT, "");
				alterForDatatype(conn, table, p, c, XSD.BOOLEAN, Datatypes.BOOLEAN,
						" USING (CASE WHEN " + c.name() + "='true' THEN true ELSE false END)");
				alterForDatatype(conn, table, p, c, XSD.DOUBLE, Datatypes.DOUBLE, "");
				alterForDatatype(conn, table, p, c, XSD.FLOAT, Datatypes.FLOAT, "");
				alterForDatatype(conn, table, p, c, XSD.DATE, Datatypes.DATE, "");
			}
		}
	}

	public static void alterForDatatype(Connection conn, Table table, PredicateMap p, Column c, final IRI xsd,
			final Datatypes sql, String cast) throws SQLException {
		if (c.isPhysical() && c.name().endsWith(Columns.LIT_VALUE)) {
			if (xsd.equals(p.datatype())) {
				alterTableTo(table, c, sql, conn, cast);
			}
		}
	}

	private static void alterTableTo(Table table, Column c, Datatypes dt, Connection conn, String cast)
			throws SQLException {
//		debug(table, c, conn);
		final String sql = "ALTER TABLE " + table.name() + " ALTER " + c.name() + " TYPE " + dt.label() + cast;
		try (Statement stat = conn.createStatement()) {
			logger.info("RUNNING " + sql);
			stat.execute(sql);
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			logger.info(
					"FAILED to convert column:" + table.name() + '.' + c.name() + " to a " + c + " using cast:" + cast);
		}
	}

//	private static void debug(Table table, Column c, Connection conn) throws SQLException {
//		try (Statement stat = conn.createStatement()) {
//			final String sql = "SELECT " + c.name() + " FROM " + table.name();
//			try (ResultSet rs= stat.executeQuery(sql)){
//				while(rs.next()) {
//					logger.info(""+rs.getString(1));
//				}
//			}
//		}
//	}
}