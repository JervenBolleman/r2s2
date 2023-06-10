package swiss.sib.swissprot.r2s2.loading.steps;

import static swiss.sib.swissprot.r2s2.DuckDBUtil.checkpoint;
import static swiss.sib.swissprot.r2s2.DuckDBUtil.open;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import swiss.sib.swissprot.r2s2.optimization.IntroduceVirtualColumns;
import swiss.sib.swissprot.r2s2.optimization.OptimizeForDatatype;
import swiss.sib.swissprot.r2s2.optimization.OptimizeForLongestCommonSubstring;
import swiss.sib.swissprot.r2s2.optimization.RdfTypeSplitting;
import swiss.sib.swissprot.r2s2.sql.Table;

public record OptimizeForR2RML(String dbPath, List<Table> tables, Map<String, String> namespaces) {

	private static final List<BiConsumer<Connection, Table>> OPTIMIZERS = List.of(IntroduceVirtualColumns::optimize,
			OptimizeForDatatype::optimize, OptimizeForLongestCommonSubstring::optimize);
	public List<Table> run() throws SQLException {
		List<Table> tables = tables();
		try (Connection conn = open(dbPath)) {
			tables = RdfTypeSplitting.split(conn, tables, namespaces);
			checkpoint(conn);
		}
		for (Table table : tables) {
			for (BiConsumer<Connection, Table> optimizer : OPTIMIZERS) {
				try (Connection conn = open(dbPath)) {
					optimizer.accept(conn, table);
					checkpoint(conn);
				}
			}
		}
		return tables;
	}
}
