package swiss.sib.swissprot.r2s2.optimization;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.JdbcUtil;
import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.GroupOfColumns;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.SqlDatatype;
import swiss.sib.swissprot.r2s2.sql.Table;

public class OptimizeForDatatype {
	private static final Logger logger = LoggerFactory.getLogger(OptimizeForDatatype.class);

	public static void optimize(Connection conn, Table table) {
		for (PredicateMap p : table.objects()) {
			if (p.objectKind() == Kind.LITERAL) {
				optimizeLiteral(conn, table, p);
			} else if (p.objectKind() == Kind.IRI) {
				optimizeIRI(conn, table.name(), p.groupOfColumns().columns());
			}
		}
		if (table.subjectKind() == Kind.IRI) {
			optimizeIRI(conn, table.name(), table.subject().columns());
		}
	}

	static void optimizeIRI(Connection conn, String tableName, List<Column> columns) {
		for (Column c : columns) {
			if (c.name().endsWith(GroupOfColumns.PORT)) {
				alterTableTo(tableName, c, SqlDatatype.INTEGER, conn, "");
			} else if (c.name().endsWith(GroupOfColumns.QUERY) || c.name().endsWith(GroupOfColumns.FRAGMENT)
					|| c.name().endsWith(GroupOfColumns.PATH)) {
				// if a such a part looks like a number and does not have leading zeros
				// we can replace such a text column with a number column.
				if (noLeadingZerosAndAllNumbers(tableName, c.name(), conn)) {
					alterTableTo(tableName, c, SqlDatatype.INTEGER, conn, "");
				}
			}
		}
	}

	private static boolean noLeadingZerosAndAllNumbers(String tableName, String name, Connection conn) {
		String sql = "SELECT EXISTS (SELECT " + name + " FROM " + tableName + " WHERE len(" + name + "::integer)=len("
				+ name + "))";
		try (Statement stat = conn.createStatement()) {
			logger.info("RUNNING " + sql);
			try (ResultSet rs = stat.executeQuery(sql)) {
				rs.next();
				return rs.getBoolean(1);
			}
		} catch (SQLException e) {
			logger.info("Column:" + tableName + '.' + name + " can not be converted to just ints");
		}
		return false;
	}

	static void optimizeLiteral(Connection conn, Table table, PredicateMap p) {
		for (Column c : p.groupOfColumns().columns()) {
			alterForDatatype(conn, table, p, c, XSD.INT, SqlDatatype.INTEGER, "");
			alterForDatatype(conn, table, p, c, XSD.INTEGER, SqlDatatype.NUMERIC, "");
			alterForDatatype(conn, table, p, c, XSD.LONG, SqlDatatype.BIGINT, "");
			alterForDatatype(conn, table, p, c, XSD.BOOLEAN, SqlDatatype.BOOLEAN,
					" USING (CASE WHEN " + c.name() + "='true' THEN true ELSE false END)");
			alterForDatatype(conn, table, p, c, XSD.DOUBLE, SqlDatatype.DOUBLE, "");
			alterForDatatype(conn, table, p, c, XSD.FLOAT, SqlDatatype.FLOAT, "");
			alterForDatatype(conn, table, p, c, XSD.DATE, SqlDatatype.DATE, "");
			alterForDatatype(conn, table, p, c, XSD.DECIMAL, SqlDatatype.NUMERIC, "");
		}
	}

	static void alterForDatatype(Connection conn, Table table, PredicateMap p, Column c, final IRI xsd,
			final SqlDatatype sql, String cast) {
		if (c.isPhysical() && c.name().endsWith(GroupOfColumns.LIT_VALUE)) {
			if (xsd.equals(p.datatype()) && c.sqlDatatype() != sql) {
				alterTableTo(table.name(), c, sql, conn, cast);
			}
		}
	}

	private static void alterTableTo(String tableName, Column c, SqlDatatype dt, Connection conn, String cast) {
//		debug(table, c, conn);
		final String sql = "ALTER TABLE " + tableName + " ALTER " + c.name() + " TYPE " + dt.label() + cast;
		try (Statement stat = conn.createStatement()) {
			logger.info("RUNNING " + sql);
			stat.execute(sql);
			JdbcUtil.commitIfNeeded(conn);
			c.setDatatype(dt);
		} catch (SQLException e) {
			logger.info(
					"FAILED to convert column:" + tableName + '.' + c.name() + " to a " + c + " using cast:" + cast);
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
