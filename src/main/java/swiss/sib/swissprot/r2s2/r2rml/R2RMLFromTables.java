package swiss.sib.swissprot.r2s2.r2rml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.R2RML;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.Columns;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;
import swiss.sib.swissprot.r2s2.sql.VirtualSingleValueColumn;

public class R2RMLFromTables {
	private static SimpleValueFactory vf = SimpleValueFactory.getInstance();

	private static Model model(List<Table> tables) {
		Model model = new LinkedHashModel();
		Resource tripleMap = vf.createBNode();

		for (var t : tables) {
			model.addAll(generateR2RML(t, tripleMap));
		}
		return model;
	}

	public static void write(List<Table> tables, File descriptionPath) throws IOException {

		final Model model = model(tables);
		try (BufferedWriter out = Files.newBufferedWriter(descriptionPath.toPath())) {
			ModelWritingHelper.writeModel(model, out);
		}
	}

	public static void write(List<Table> tables, OutputStream os) throws IOException {
		final List<Table> copyOf = new ArrayList<>(tables);
		copyOf.sort((a,b)->a.name().compareTo(b.name()));
		final Model model = model(tables);
		try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(os))) {
			ModelWritingHelper.writeModel(model, out);
		}
	}

	public static Model generateR2RML(Table t, Resource tripleMap) {
		Model model = new LinkedHashModel();
		Resource table = vf.createBNode();// "table_" + name());
		Resource tablename = vf.createBNode();// "tablename_" + name());
		Resource subjectMap = vf.createBNode();// "subject_" + name());
		model.add(vf.createStatement(table, RDF.TYPE, R2RML.TriplesMap));
		model.add(vf.createStatement(table, R2RML.logicalTable, tablename));
		model.add(vf.createStatement(tablename, R2RML.tableName, vf.createLiteral("main."+t.name())));
		model.add(vf.createStatement(table, R2RML.subjectMap, subjectMap));

		createTemplate(model, vf, subjectMap, t.subjectKind(), "subject", t.subject());

		for (PredicateMap p : t.objects()) {
			createPredicateMap(model, vf, table, p);
		}
		return model;
	}

	private static void createPredicateMap(Model model, SimpleValueFactory vf, Resource table, PredicateMap p) {
		Resource predicateMap = vf.createBNode();// "predicateMap_" + name());
		Resource objectMap = vf.createBNode();// "objectMap_" + name());
		if (p.predicate().equals(RDF.TYPE)) {
			boolean allVirtual = true;
			StringBuilder template = new StringBuilder();

			for (Column c : p.columns().getColumns()) {
				if (!c.isVirtual()) {
					allVirtual = false;
				} else if (!c.name().endsWith(Columns.GRAPH)) {
					template.append(((VirtualSingleValueColumn) c).value());
				} else {
					addGraphs(model, vf, table, c);
				}
			}
			if (allVirtual) {
				model.add(vf.createStatement(table, R2RML.clazz, vf.createIRI(template.toString())));
				return;
			}
		}
		model.add(vf.createStatement(table, R2RML.predicateObjectMap, predicateMap));
		model.add(vf.createStatement(predicateMap, R2RML.predicate, p.predicate()));
		model.add(vf.createStatement(predicateMap, R2RML.objectMap, objectMap));

		createTemplate(model, vf, objectMap, p.objectKind(), "object", p.columns());

	}

	private static void createTemplate(Model model, SimpleValueFactory vf, Resource map, Kind k, String n, Columns c) {
		model.add(vf.createStatement(map, R2RML.termType, asR2RMLTermType(k)));
		if (k == Kind.LITERAL) {
			for (Column column : c.getColumns()) {
				if (!column.name().endsWith(Columns.GRAPH)) {
					if (column.name().endsWith(Columns.DATATYPE)) {
						columnDefinition(model, vf, R2RML.datatype, map, column, vf::createIRI);
					} else if (column.name().endsWith(Columns.LANG)) {
						columnDefinition(model, vf, R2RML.language, map, column, vf::createLiteral);
					} else if (column.name().endsWith(Columns.LANG_VALUE)) {
						columnDefinition(model, vf, R2RML.column, map, column, vf::createLiteral);
					} else if (column.name().endsWith(Columns.LIT_VALUE)) {
						columnDefinition(model, vf, R2RML.column, map, column, vf::createLiteral);
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
						template.append(((VirtualSingleValueColumn) column).value());
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
					if (column.isPhysical()) {
						model.add(map, R2RML.column, vf.createLiteral(column.name()));
					} else {
						model.add(map, R2RML.template, vf.createLiteral(((VirtualSingleValueColumn) column).value()));
					}
				} else {
					addGraphs(model, vf, map, column);
				}
			}
		}
	}

	public static void addGraphs(Model model, SimpleValueFactory vf, Resource map, Column column) {
		if (column.isVirtual()) {
			final VirtualSingleValueColumn virtualColumn = (VirtualSingleValueColumn) column;
			model.add(vf.createStatement(map, R2RML.graph, vf.createIRI(virtualColumn.value())));
		} else {
			Resource graphs = vf.createBNode();
			model.add(vf.createStatement(map, R2RML.graphMap, graphs));
			model.add(vf.createStatement(graphs, R2RML.template, vf.createLiteral('{' + column.name() + '}')));
		}
	}

	private static void columnDefinition(Model model, SimpleValueFactory vf, IRI p, Resource map, Column column, Function<String, Value> create) {
		if (column.isVirtual()) {
			model.add(map, p, create.apply(((VirtualSingleValueColumn) column).value()));
		} else {
			model.add(map, p, vf.createLiteral(column.name()));
		}
	}

	private static IRI asR2RMLTermType(Kind k) {
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
}
