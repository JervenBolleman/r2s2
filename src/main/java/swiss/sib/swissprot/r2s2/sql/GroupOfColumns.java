package swiss.sib.swissprot.r2s2.sql;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.duckdb.DuckDBAppender;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.loading.LoaderBlankNode;

public record GroupOfColumns(List<Column> columns) {
	public static final String DATATYPE = "_datatype";
	public static final String LIT_VALUE = "_litvalue";
	public static final String LANG_VALUE = "_langvalue";
	public static final String LANG = "_lang";
	public static final String ID = "_id";
	public static final String IRI = "_iri";
	public static final String BNODE = "_bnode";
	public static final String PARTS = "_parts";
	public static final String PROTOCOL = "_protocol";
	public static final String GRAPH = "_graph";
	public static final String HOST = "_host";

	public GroupOfColumns(List<Column> columns) {
		this.columns = new CopyOnWriteArrayList<>(columns);
	}

	public String definition() {
		return columns.stream().filter(Column::isPhysical).map(Column::definition).collect(Collectors.joining(", "));
	}

	public static GroupOfColumns from(Kind kind, String lang, IRI datatype, String prefix, Map<String, String> namespaces,
			IRI predicate) {
		final String predicatePart = Naming.iriToSqlNamePart(namespaces, predicate);
		switch (kind) {
		case IRI:
			return new GroupOfColumns(List.of(new Column(prefix + predicatePart + PROTOCOL, SqlDatatype.TEXT),
					new Column(prefix + predicatePart + HOST, SqlDatatype.TEXT),
					new Column(prefix + predicatePart + PARTS, SqlDatatype.TEXT)));
		case BNODE:
			return new GroupOfColumns(List.of(new Column(prefix + predicatePart + ID, SqlDatatype.BIGINT)));
		case LITERAL:
			if (lang != null) {
				return new GroupOfColumns(List.of(new Column(prefix + predicatePart + LANG, SqlDatatype.TEXT),
						new Column(prefix + predicatePart + LANG_VALUE, SqlDatatype.TEXT)));
			} else if (datatype != null) {
				final String datatypePart = predicatePart + Naming.iriToSqlNamePart(namespaces, datatype);
				Column datatypeColumn = new Column(prefix + datatypePart + DATATYPE, SqlDatatype.TEXT);
				final Column valueColumn = new Column(prefix + datatypePart + LIT_VALUE, SqlDatatype.TEXT);
				return new GroupOfColumns(List.of(datatypeColumn, valueColumn));
			} else {
				return null;
			}
		case TRIPLE:
		default:
			throw new UnsupportedOperationException();
		}
	}

	public static Column graphColumn(Kind kind, String lang, IRI datatype, String prefix,
			Map<String, String> namespaces, IRI predicate) {
		final String predicatePart = Naming.iriToSqlNamePart(namespaces, predicate);
		switch (kind) {
		case IRI:
			return new Column(prefix + predicatePart + IRI + GRAPH, SqlDatatype.INTEGER);
		case BNODE:
			return new Column(prefix + predicatePart + BNODE + GRAPH, SqlDatatype.INTEGER);
		case LITERAL:
			if (lang != null) {
				return new Column(prefix + predicatePart + LANG + GRAPH, SqlDatatype.INTEGER);
			} else if (datatype != null) {
				final String datatypePart = predicatePart + Naming.iriToSqlNamePart(namespaces, datatype);
				return new Column(prefix + datatypePart + DATATYPE + GRAPH, SqlDatatype.TEXT);
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

	public void add(Value v, int tempGraphId, DuckDBAppender appender) throws SQLException {
		add(v, appender);
		appender.append(tempGraphId);
	}

	public Optional<Column> getColumn(String name) {
		for (Column col : columns) {
			if (name.equals(col.name()))
				return Optional.of(col);
		}
		return Optional.empty();
	}
	
	public GroupOfColumns copy() {
		
		return new GroupOfColumns(columns.stream().map(Column::copy).collect(Collectors.toList()));
	}
}
