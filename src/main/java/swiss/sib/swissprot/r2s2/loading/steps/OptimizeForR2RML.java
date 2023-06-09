package swiss.sib.swissprot.r2s2.loading.steps;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static swiss.sib.swissprot.r2s2.DuckDBUtil.open;
import static swiss.sib.swissprot.r2s2.DuckDBUtil.checkpoint;
import swiss.sib.swissprot.r2s2.analysis.IntroduceVirtualColumns;
import swiss.sib.swissprot.r2s2.analysis.OptimizeForDatatype;
import swiss.sib.swissprot.r2s2.analysis.OptimizeForLongestCommonSubstring;
import swiss.sib.swissprot.r2s2.analysis.RdfTypeSplitting;
import swiss.sib.swissprot.r2s2.analysis.TableMerging;
import swiss.sib.swissprot.r2s2.sql.Table;

public record OptimizeForR2RML(String dbPath, List<Table> tables, Map<String, String> namespaces) {
	public List<Table> run() throws SQLException {
		List<Table> tables = tables();
		try (Connection conn = open(dbPath)) {
			RdfTypeSplitting rdfTypeSplitting = new RdfTypeSplitting();
			tables = rdfTypeSplitting.split(conn, tables, namespaces);
			checkpoint(conn);
		}
		for (Table table : tables) {
			try (Connection conn = open(dbPath)) {
				IntroduceVirtualColumns.optimizeForR2RML(conn, table);
				checkpoint(conn);
			}
		}
		for (Table table : tables) {
			try (Connection conn = open(dbPath)) {
				OptimizeForDatatype.optimizeForR2RML(conn, table);
				checkpoint(conn);
			}
		}

		for (Table table : tables) {
			try (Connection conn = open(dbPath)) {
				OptimizeForLongestCommonSubstring.optimizeForR2RML(conn, table);
				checkpoint(conn);
			}
		}
		try (Connection conn = open(dbPath)) {
			tables = new TableMerging().merge(conn, tables);
			checkpoint(conn);
		}
		return tables;
	}
}
