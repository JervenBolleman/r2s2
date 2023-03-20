package swiss.sib.swissprot.r2s2.sql;

import java.sql.SQLException;
import java.util.Objects;

import org.duckdb.DuckDBAppender;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

public class Column {
	private final String name;
	private final Datatypes datatype;
	private final Model model = new LinkedHashModel();
	
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

	public Model model() {
		return model;
	}
	
	public boolean isVirtual() {
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(datatype, model, name);
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
		return datatype == other.datatype && Objects.equals(model, other.model) && Objects.equals(name, other.name);
	}
}
