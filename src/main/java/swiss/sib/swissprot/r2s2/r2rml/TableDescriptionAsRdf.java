package swiss.sib.swissprot.r2s2.r2rml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.List;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.R2RML;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.TableAsRdf;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;

import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
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
		for (Column c : t.subject().getColumns()) {
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
			for (Column c : p.columns().getColumns()) {
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
			model.add(vf.createStatement(subjectColumn, RDFS.LABEL, vf.createLiteral(c.name())));
			model.add(vf.createStatement(subjectColumn, RDF.VALUE, vf.createLiteral(c.datatype().label())));
		} else {
			model.add(vf.createStatement(subjectColumn, RDF.TYPE, TableAsRdf.virtualColumn));
			VirtualSingleValueColumn vc = (VirtualSingleValueColumn) c;
			model.add(vf.createStatement(subjectColumn, RDF.VALUE, vf.createLiteral(vc.value())));
		}
	}

	public static void write(List<Table> tables, File descriptionPath) throws IOException {
		
		try (BufferedWriter out= Files.newBufferedWriter(descriptionPath.toPath())){
			RDFWriter r2rmlWriter = Rio.createWriter(RDFFormat.TURTLE, out);
			WriterConfig writerConfig = r2rmlWriter.getWriterConfig();
			writerConfig.set(BasicWriterSettings.PRETTY_PRINT, Boolean.TRUE);
			writerConfig.set(BasicWriterSettings.INLINE_BLANK_NODES, Boolean.TRUE);
			r2rmlWriter.startRDF();
			r2rmlWriter.handleNamespace(R2RML.PREFIX, R2RML.NAMESPACE);
			r2rmlWriter.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
			r2rmlWriter.handleNamespace(RDFS.PREFIX, RDFS.NAMESPACE);
			for (var s : model(tables)) {
				r2rmlWriter.handleStatement(s);
			}
			r2rmlWriter.endRDF();
		}
	}
}
