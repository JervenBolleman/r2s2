package swiss.sib.swissprot.r2s2.sql;

import java.sql.SQLException;
import java.util.Objects;

import org.duckdb.DuckDBAppender;

public class Column {
	private final String name;
	private Datatypes datatype;
	
	
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

	public String name() {
		return name;
	}

	public Datatypes datatype() {
		return datatype;
	}
	
	public boolean isVirtual() {
		return false;
	}
	
	public final boolean isPhysical() {
		return ! isVirtual();
	}

	@Override
	public int hashCode() {
		return Objects.hash(datatype, name);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Column other = (Column) obj;
		return datatype == other.datatype && Objects.equals(name, other.name);
	}

	public void setDatatype(Datatypes datatype) {
		this.datatype = datatype;
	}
}
