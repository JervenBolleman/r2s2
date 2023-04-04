package swiss.sib.swissprot.r2s2.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.R2RML;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;

public class Table {
	private static final Logger log = LoggerFactory.getLogger(Table.class);
	private final Columns subject;
	private final IRI predicate;
	private final List<PredicateMap> objects = new ArrayList<>();
	private final Column graphColumn;
	private final Kind subjectKind;
	private final Kind objectKind;
	private final String lang;
	private final IRI datatype;
	private final int id;
	private static final AtomicInteger ID_GEN = new AtomicInteger();

	public Table(IRI predicate, Columns subject, Kind subjectKind, Columns object, Kind objectKind, Column graphColumn,
			String lang, IRI datatype) {
		super();
		this.predicate = predicate;
		this.subject = subject;
		this.subjectKind = subjectKind;
		this.objects.add(new PredicateMap(predicate, object, objectKind, lang, datatype));
		this.objectKind = objectKind;
		this.graphColumn = graphColumn;
		this.lang = lang;
		this.datatype = datatype;
		this.id = ID_GEN.incrementAndGet();
	}

	public Columns subject() {
		return subject;
	}

	public List<PredicateMap> objects() {
		return objects;
	}

	public void create(Connection conn) throws SQLException {
		String objectsDefinition = objects.stream().map((p) -> p.columns().definition())
				.collect(Collectors.joining(","));
		if (objectsDefinition == null || objectsDefinition.isEmpty()) {
			objectsDefinition = "";
		} else {
			objectsDefinition = ", " + objectsDefinition;
		}
		String dml = "CREATE OR REPLACE TABLE " + name() + " (" + subject.definition() + objectsDefinition + ", "
				+ graphColumn.definition() + ")";
		try (Statement ct = conn.createStatement()) {

			LoggerFactory.getLogger(this.getClass()).warn("Running: " + dml);
			ct.execute(dml);
		}

	}

	public String name() {

		String name = "_" + id + "_" + subjectKind.label() + "_" + objectKind.label();
		if (lang != null) {
			return name + "_" + lang.replace('-', '_');
		} else if (datatype != null) {

			CoreDatatype from = CoreDatatype.from(datatype);
			if (from != null && from.isXSDDatatype()) {
				name += "_xsd_" + datatype.getLocalName();
			} else if (from != null && from.isRDFDatatype()) {
				name += "_rdf_" + datatype.getLocalName();
			} else if (from != null && from.isGEODatatype()) {
				name += "_geo_" + datatype.getLocalName();
			} else {
				name += "_dt";
			}
		}
		return name;
	}

	public Column graph() {
		return graphColumn;
	}

	

	public Model generateR2RML() {
		Model model = new LinkedHashModel();
		SimpleValueFactory vf = SimpleValueFactory.getInstance();
		Resource table = vf.createBNode();// "table_" + name());
		Resource tablename = vf.createBNode();// "tablename_" + name());
		Resource subjectMap = vf.createBNode();// "subject_" + name());

		model.add(vf.createStatement(table, R2RML.logicalTable, tablename));
		model.add(vf.createStatement(tablename, R2RML.tableName, vf.createLiteral(name())));
		model.add(vf.createStatement(table, R2RML.subjectMap, subjectMap));

		createTemplate(model, vf, subjectMap, subjectKind, "subject", subject);

		for (PredicateMap p : objects) {
			createPredicateMap(model, vf, table, p);
		}
		return model;
	}

	private void createPredicateMap(Model model, SimpleValueFactory vf, Resource table, PredicateMap p) {
		Resource predicateMap = vf.createBNode();// "predicateMap_" + name());
		Resource objectMap = vf.createBNode();// "objectMap_" + name());
		if (p.predicate().equals(RDF.TYPE)) {
			boolean allVirtual = true;
			StringBuilder template = new StringBuilder();

			for (Column c : p.columns().getColumns()) {
				if (!c.isVirtual())
					allVirtual = false;
				else
					template.append(((VirtualSingleValueColumn) c).getValue());
			}
			if (allVirtual) {
				model.add(vf.createStatement(table, R2RML.clazz, vf.createIRI(template.toString())));
				return;
			}
		}
		model.add(vf.createStatement(table, R2RML.predicateMap, predicateMap));
		model.add(vf.createStatement(predicateMap, R2RML.predicate, p.predicate()));
		model.add(vf.createStatement(predicateMap, R2RML.objectMap, objectMap));

		createTemplate(model, vf, objectMap, p.objectKind(), "object", p.columns());

	}

	private void createTemplate(Model model, SimpleValueFactory vf, Resource map, Kind k, String n, Columns c) {
		model.add(vf.createStatement(map, R2RML.termType, asR2RMLTermType(k)));
		if (k == Kind.LITERAL) {
			Optional<Column> column = c.getColumn(n + Columns.DATATYPE);
			if (!column.isEmpty()) {
				columnDefinition(model, vf, R2RML.datatype, map, column);
			}
			column = c.getColumn(n + Columns.LANG);
			if (!column.isEmpty()) {
				columnDefinition(model, vf, R2RML.language, map, column);
			}
			column = c.getColumn(n + Columns.VALUE);
			if (!column.isEmpty()) {
				columnDefinition(model, vf, R2RML.template, map, column);
			}
		} else if (k == Kind.IRI) {
			StringBuilder template = new StringBuilder();
			for (Column column : c.getColumns()) {
				if (column.isVirtual()) {
					template.append(((VirtualSingleValueColumn) column).getValue());
				} else {
					template.append('{').append(column.name()).append('}');
				}
			}
			model.add(map, R2RML.template, vf.createLiteral(template.toString()));
		} else if (k == Kind.BNODE) {

		}
	}

	private void columnDefinition(Model model, SimpleValueFactory vf, IRI p, Resource map, Optional<Column> column) {
		Column langColumn = column.get();
		if (langColumn.isVirtual()) {
			model.add(map, p, vf.createLiteral(((VirtualSingleValueColumn) langColumn).getValue()));
		} else {
			model.add(map, p, vf.createLiteral('{' + langColumn.name() + '}'));
		}
	}

	private IRI asR2RMLTermType(Kind k) {
		switch (k) {
		case BNODE:
			return R2RML.BlankNode;
		case IRI:
			return R2RML.IRI;
		case LITERAL:
			return R2RML.Literal;
		default:
			throw new IllegalArgumentException("Unexpected value: " + k);
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(datatype, graphColumn, lang, objects, objectKind, predicate, subject, subjectKind);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Table other = (Table) obj;
		return Objects.equals(datatype, other.datatype) && Objects.equals(graphColumn, other.graphColumn)
				&& Objects.equals(lang, other.lang) && Objects.equals(objects, other.objects)
				&& objectKind == other.objectKind && Objects.equals(predicate, other.predicate)
				&& Objects.equals(subject, other.subject) && subjectKind == other.subjectKind;
	}

	public Kind subjectKind() {
		return subjectKind;
	}
}
