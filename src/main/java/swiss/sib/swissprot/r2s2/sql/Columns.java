package swiss.sib.swissprot.r2s2.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.duckdb.DuckDBAppender;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.loading.LoaderBlankNode;

public class Columns {
	public static final String DATATYPE = "_datatype";
	public static final String VALUE = "_value";
	public static final String LANG = "_lang";
	public static final String ID = "_id";
	public static final String PARTS = "_parts";
	public static final String PROTOCOL = "_protocol";
	public static final String HOST = "_host";
	private final List<Column> columns;

	public Columns(List<Column> of) {
		this.columns = new CopyOnWriteArrayList<>(of);
	}

	public String definition() {
		return columns.stream().map(Column::definition).collect(Collectors.joining(", "));
	}

	public static Columns from(Kind kind, String lang, IRI datatype, String prefix) {
		switch (kind) {
		case IRI:
			return new Columns(List.of(new Column(prefix + PROTOCOL, Datatypes.TEXT),
					new Column(prefix + HOST, Datatypes.TEXT), new Column(prefix + PARTS, Datatypes.TEXT)));
		case BNODE:
			return new Columns(List.of(new Column(prefix + ID, Datatypes.BIGINT)));
		case LITERAL:
			if (lang != null) {
				return new Columns(List.of(new Column(prefix + LANG, Datatypes.TEXT),
						new Column(prefix + VALUE, Datatypes.TEXT)));
			} else if (datatype != null) {
				return new Columns(List.of(new Column(prefix + DATATYPE, Datatypes.TEXT),
						new Column(prefix + VALUE, Datatypes.TEXT)));
			} else {
				return null;
			}
		case TRIPLE:
		default:
			throw new UnsupportedOperationException();
		}
	}

	public void add(Value subjectS, DuckDBAppender appender) throws SQLException {
		if (subjectS.isIRI()) {
			String i = subjectS.stringValue();
			String protocol = i.substring(0, i.indexOf("://") + 3);
			String host = i.substring(protocol.length(), i.indexOf('/', protocol.length()));
			String q = i.substring(protocol.length() + host.length());
			appender.append(protocol);
			appender.append(host);
			appender.append(q);
		} else if (subjectS.isBNode()) {
			long i = ((LoaderBlankNode) subjectS).id();
			appender.append(i);
		} else if (subjectS.isLiteral()) {
			Literal l = (Literal) subjectS;
			if (l.getLanguage().isPresent()) {
				appender.append(l.getLanguage().get());
			} else {
				IRI datatype = l.getDatatype();
				appender.append(datatype.stringValue());
			}
			appender.append(l.stringValue());
		}

	}

	public List<Column> getColumns() {
		return columns;
	}

	public Optional<Column> getColumn(String name) {
		for (Column col : columns) {
			if (name.equals(col.name()))
				return Optional.of(col);
		}
		return Optional.empty();
	}

	@Override
	public int hashCode() {
		return Objects.hash(columns);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Columns other = (Columns) obj;
		return Objects.equals(columns, other.columns);
	}
}
