package swiss.sib.swissprot.r2s2.r2rml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.TableAsRdf;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.GroupOfColumns;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.SqlDatatype;
import swiss.sib.swissprot.r2s2.sql.Table;
import swiss.sib.swissprot.r2s2.sql.VirtualSingleValueColumn;

public class TableDescriptionAsRdf {
	public static Model model(List<Table> tables) {
		Model model = new LinkedHashModel();
		for (var t : tables) {
			model.addAll(generateRdfDecription(t));
		}
		return model;
	}

	public static Model generateRdfDecription(Table t) {
		Model model = new LinkedHashModel();
		SimpleValueFactory vf = SimpleValueFactory.getInstance();
		Resource table = vf.createBNode();// "table_" + name());
		Resource subjectColumns = vf.createBNode();// "tablename_" + name());

		model.add(vf.createStatement(table, RDF.TYPE, TableAsRdf.table));
		model.add(vf.createStatement(table, RDFS.LABEL, vf.createLiteral(t.name())));
		model.add(vf.createStatement(table, TableAsRdf.subjectColumns, subjectColumns));
		model.add(vf.createStatement(table, TableAsRdf.kind, vf.createLiteral(t.subjectKind().label())));
		for (Column c : t.subject().columns()) {
			Resource subjectColumn = vf.createBNode();// "tablename_" + name());
			addColumn(model, vf, subjectColumns, c, subjectColumn);
		}

		for (PredicateMap p : t.objects()) {
			Resource objectColumns = vf.createBNode();
			model.add(vf.createStatement(table, TableAsRdf.objectColumns, objectColumns));
			model.add(vf.createStatement(objectColumns, TableAsRdf.predicate, p.predicate()));
			model.add(vf.createStatement(objectColumns, TableAsRdf.kind, vf.createLiteral(p.objectKind().label())));
			if (p.datatype() != null)
				model.add(vf.createStatement(objectColumns, TableAsRdf.datatype, p.datatype()));
			if (p.lang() != null)
				model.add(vf.createStatement(objectColumns, TableAsRdf.lang, vf.createLiteral(p.lang())));
			for (Column c : p.groupOfColumns().columns()) {
				Resource objectColumn = vf.createBNode();// "tablename_" + name());
				addColumn(model, vf, objectColumns, c, objectColumn);
			}
		}
		return model;
	}

	public static void addColumn(Model model, SimpleValueFactory vf, Resource subjectColumns, Column c,
			Resource subjectColumn) {
		model.add(vf.createStatement(subjectColumns, TableAsRdf.column, subjectColumn));
		if (c.isPhysical()) {
			model.add(vf.createStatement(subjectColumn, RDF.TYPE, TableAsRdf.physicalColumn));
		} else {
			VirtualSingleValueColumn vc = (VirtualSingleValueColumn) c;
			if (vc.value() !=null) {
				model.add(vf.createStatement(subjectColumn, RDF.TYPE, TableAsRdf.virtualColumn));
				model.add(vf.createStatement(subjectColumn, RDF.VALUE, vf.createLiteral(vc.value())));
			}
		}
		model.add(vf.createStatement(subjectColumn, RDFS.LABEL, vf.createLiteral(c.name())));
		model.add(vf.createStatement(subjectColumn, TableAsRdf.datatype, vf.createLiteral(c.sqlDatatype().label())));
	}

	public static void write(List<Table> tables, File descriptionPath) throws IOException {

		final Model model = model(tables);
		try (BufferedWriter out = Files.newBufferedWriter(descriptionPath.toPath())) {
			ModelWritingHelper.writeModel(model, out);
		}
	}

	public static void write(List<Table> tables, OutputStream os) throws IOException {

		final Model model = model(tables);
		try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(os))) {
			ModelWritingHelper.writeModel(model, out);
		}
	}

	public static List<Table> tables(Model model) {
		List<Table> tables = new ArrayList<>();

		for (var tableBnode : model.getStatements(null, RDF.TYPE, TableAsRdf.table)) {
			Table table = null;
			String tableName = findTableName(model, tableBnode);
			List<Column> subjectColumns = findColumns(model, tableBnode);
			Kind subjectKind = findKind(model, tableBnode.getSubject());
			List<PredicateMap> opm = findPredicateMaps(model, tableBnode);
			table = new Table(tableName, new GroupOfColumns(subjectColumns), subjectKind, opm);
			tables.add(table);
		}
		return tables;
	}

	private static List<PredicateMap> findPredicateMaps(Model model, Statement tableBnode) {
		List<PredicateMap> pms = new ArrayList<>();
		for (Statement columnsS : model.getStatements(tableBnode.getSubject(), TableAsRdf.objectColumns, null)) {
			Resource cbn = (Resource) columnsS.getObject();
			List<Column> columnList = findColumn(model, cbn);
			Kind objectKind = findKind(model, cbn);
			IRI predicate = readIriFrom(model, TableAsRdf.predicate, cbn);
			String lang = readStringFrom(model, TableAsRdf.lang, cbn);
			IRI datatype = readIriFrom(model, TableAsRdf.datatype, cbn);
			pms.add(new PredicateMap(predicate, new GroupOfColumns(columnList), objectKind, lang, datatype));
		}
		return pms;
	}

	private static String readStringFrom(Model model, IRI lang, Resource cbn) {
		for (var subjectColumns : model.getStatements(cbn, lang, null)) {
			return subjectColumns.getObject().stringValue();
		}
		return null;
	}

	private static IRI readIriFrom(Model model, IRI iri, Resource cbn) {
		for (var subjectColumns : model.getStatements(cbn, iri, null)) {
			return SimpleValueFactory.getInstance().createIRI(subjectColumns.getObject().stringValue());
		}
		return null;
	}

	private static Kind findKind(Model model, Resource tableBnode) {
		Kind subjectKind = null;
		for (var subjectColumns : model.getStatements(tableBnode, TableAsRdf.kind, null)) {
			return Kind.fromLabel(subjectColumns.getObject().stringValue());
		}
		return subjectKind;
	}

	private static List<Column> findColumns(Model model, Statement tableBnode) {
		List<Column> columnList = new ArrayList<>();
		for (var subjectColumns : model.getStatements(tableBnode.getSubject(), TableAsRdf.subjectColumns, null)) {
			Resource cbn = (Resource) subjectColumns.getObject();
			List<Column> c = findColumn(model, cbn);
			columnList.addAll(c);
		}
		return columnList;
	}

	private static List<Column> findColumn(Model model, Resource cbn) {
		List<Column> columns = new ArrayList<>();
		for (Statement columnS : model.getStatements(cbn, TableAsRdf.column, null)) {
			Resource s = (Resource) columnS.getObject();
			boolean isPhysical = false;
			String columnName = null;
			SqlDatatype datatype = null;
			for (Statement columnType : model.getStatements(s, RDF.TYPE, null)) {
				isPhysical = TableAsRdf.physicalColumn.equals(columnType.getObject());
			}

			for (Statement columnTypeS : model.getStatements(s, RDFS.LABEL, null)) {
				columnName = columnTypeS.getObject().stringValue();
			}
			for (Statement columnTypeS : model.getStatements(s, TableAsRdf.datatype, null)) {
				datatype = SqlDatatype.fromLabel(columnTypeS.getObject().stringValue());
			}
			if (isPhysical) {
				columns.add(new Column(columnName, datatype));
			} else {
				for (Statement columnTypeS : model.getStatements(s, RDF.VALUE, null)) {
					String singleValue = columnTypeS.getObject().stringValue();
					columns.add(new VirtualSingleValueColumn(columnName, datatype, singleValue));
				}
			}
		}
		return columns;
	}

	private static String findTableName(Model model, Statement tableBnode) {
		String tableName = null;
		for (var tableNameS : model.getStatements(tableBnode.getSubject(), RDFS.LABEL, null)) {
			tableName = tableNameS.getObject().stringValue();
		}
		return tableName;
	}

	public static List<Table> read(File descriptionPath) throws IOException {
		final Optional<RDFFormat> format = Rio.getParserFormatForFileName(descriptionPath.getName());
		if (format.isPresent()) {
			try (InputStream is = Files.newInputStream(descriptionPath.toPath())) {
				final Model parse = Rio.parse(is, format.get());
				return tables(parse);
			}
		}
		return null;
	}
}
