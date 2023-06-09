package swiss.sib.swissprot.r2s2.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
	private final List<PredicateMap> objects = new ArrayList<>();

	private final Kind subjectKind;
	private final String name;
	private static final AtomicInteger ID_GEN = new AtomicInteger();

	public Table(IRI predicate, Columns subject, Kind subjectKind, Columns object, Kind objectKind, String lang,
			IRI datatype) {
		super();
		this.subject = subject;
		this.subjectKind = subjectKind;
		this.objects.add(new PredicateMap(predicate, object, objectKind, lang, datatype));

		this.name = generateName("_pred_" + ID_GEN.incrementAndGet(), subjectKind, objectKind, lang, datatype);
	}

	public static String generateName(String name, Kind subjectKind, Kind objectKind, String lang, IRI datatype) {
		name = name + "_" + subjectKind.label() + "_" + objectKind.label();
		name = addLangDatatype(lang, datatype, name);
		return name;
	}

	public static String addLangDatatype(String lang, IRI datatype, String name) {
		if (lang != null) {
			name += "_" + lang.replace('-', '_');
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

	public Table(String name, Columns subject, Kind subjectKind, List<PredicateMap> objects) {
		super();
		this.subject = subject;
		this.subjectKind = subjectKind;
		this.objects.addAll(objects);
		this.name = name;
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
		String dml = "CREATE TABLE " + name() + " (" + subject.definition() + objectsDefinition + ")";
		try (Statement ct = conn.createStatement()) {

			log.warn("Running: " + dml);
			ct.execute(dml);
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		}

	}

	public String name() {

		return name;
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
				if (!c.isVirtual()) {
					allVirtual = false;
				} else if (!c.name().endsWith(Columns.GRAPH)) {
					template.append(((VirtualSingleValueColumn) c).getValue());
				} else  {
					addGraphs(model, vf, table, c);
				}
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
			for (Column column : c.getColumns()) {
				if (!column.name().endsWith(Columns.GRAPH)) {
					if (column.name().endsWith(Columns.DATATYPE)) {
						columnDefinition(model, vf, R2RML.datatype, map, column);
					} else if (column.name().endsWith(Columns.LANG)) {
						columnDefinition(model, vf, R2RML.language, map, column);
					} else if (column.name().endsWith(Columns.LANG_VALUE)) {
						columnDefinition(model, vf, R2RML.template, map, column);
					} else if (column.name().endsWith(Columns.LIT_VALUE)) {
						columnDefinition(model, vf, R2RML.template, map, column);
					}
				} else {
					addGraphs(model, vf, map, column);
				}
			}
		} else if (k == Kind.IRI) {
			StringBuilder template = new StringBuilder();
			for (Column column : c.getColumns()) {
				if (!column.name().endsWith(Columns.GRAPH)) {
					if (column.isVirtual()) {
						template.append(((VirtualSingleValueColumn) column).getValue());
					} else {
						template.append('{').append(column.name()).append('}');
					}
				} else {
					addGraphs(model, vf, map, column);
				}
			}
			model.add(map, R2RML.template, vf.createLiteral(template.toString()));
		} else if (k == Kind.BNODE) {
			for (Column column : c.getColumns()) {
				if (!column.name().endsWith(Columns.GRAPH)) {
					model.add(map, R2RML.template, vf.createLiteral('{'+column.name()+'}'));
				} else {
					addGraphs(model, vf, map, column);
				}
			}
		}
	}

	public void addGraphs(Model model, SimpleValueFactory vf, Resource map, Column column) {
		if (column.isVirtual()) {
			final VirtualSingleValueColumn virtualColumn = (VirtualSingleValueColumn) column;
			model.add(vf.createStatement(map, R2RML.graph, vf.createLiteral(virtualColumn.getValue())));
		} else { 
			Resource graphs = vf.createBNode(); 
			model.add(vf.createStatement(map, R2RML.graphMap, graphs));
			model.add(vf.createStatement(graphs, R2RML.template, vf.createLiteral('{'+column.name()+'}')));
		}
	}

	private void columnDefinition(Model model, SimpleValueFactory vf, IRI p, Resource map, Column column) {
		if (column.isVirtual()) {
			model.add(map, p, vf.createLiteral(((VirtualSingleValueColumn) column).getValue()));
		} else {
			model.add(map, p, vf.createLiteral('{' + column.name() + '}'));
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
		return Objects.hash(objects, name, subject, subjectKind);
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
		return Objects.equals(objects, other.objects) && name == other.name && Objects.equals(name, other.name)
				&& Objects.equals(subject, other.subject) && subjectKind == other.subjectKind;
	}

	public Kind subjectKind() {
		return subjectKind;
	}
}
