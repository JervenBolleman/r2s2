package swiss.sib.swissprot.r2s2.sql;

import java.sql.SQLException;

import org.duckdb.DuckDBAppender;

public class Column {
	public String name;
	public Datatypes datatype;

	public Column(String name, Datatypes datatype) {
		this.name = name;
		this.datatype = datatype;
	}

	public String definition() {
		return name + " " + datatype.label();
	}

	public void add(int tempGraphId, DuckDBAppender appender) throws SQLException {
		appender.append(tempGraphId);
	}
}
