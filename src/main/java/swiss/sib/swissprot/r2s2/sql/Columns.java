package swiss.sib.swissprot.r2s2.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.duckdb.DuckDBAppender;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.loading.LoaderBlankNode;

public class Columns {
	private final List<Column> columns;

	public Columns(List<Column> of) {
		this.columns = new ArrayList<>(of);
	}

	public String definition() {
		return columns.stream().map(Column::definition).collect(Collectors.joining(", "));
	}

	public static Columns from(Kind kind, String lang, IRI datatype, String prefix) {
		switch (kind) {
		case IRI:
			return new Columns(List.of(new Column(prefix + "_protocol", Datatypes.TEXT),
					new Column(prefix + "_host", Datatypes.TEXT), new Column(prefix + "_parts", Datatypes.TEXT)));
		case BNODE:
			return new Columns(List.of(new Column(prefix + "_id", Datatypes.BIGINT)));
		case LITERAL:
			if (lang != null) {
				return new Columns(List.of(new Column(prefix + "_lang", Datatypes.TEXT),
						new Column(prefix + "_value", Datatypes.TEXT)));
			} else if (datatype != null) {
				return new Columns(List.of(new Column(prefix + "_datatype", Datatypes.TEXT),
						new Column(prefix + "_value", Datatypes.TEXT)));
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
}
