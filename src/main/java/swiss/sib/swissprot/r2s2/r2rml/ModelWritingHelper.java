package swiss.sib.swissprot.r2s2.r2rml;

import java.io.Writer;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.vocabulary.R2RML;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;

public class ModelWritingHelper {

	public static void writeModel(final Model model, Writer out) {
		RDFWriter r2rmlWriter = Rio.createWriter(RDFFormat.TURTLE, out);
		WriterConfig writerConfig = r2rmlWriter.getWriterConfig();
		writerConfig.set(BasicWriterSettings.PRETTY_PRINT, Boolean.TRUE);
		writerConfig.set(BasicWriterSettings.INLINE_BLANK_NODES, Boolean.TRUE);
		r2rmlWriter.startRDF();
		r2rmlWriter.handleNamespace(R2RML.PREFIX, R2RML.NAMESPACE);
		r2rmlWriter.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
		r2rmlWriter.handleNamespace(RDFS.PREFIX, RDFS.NAMESPACE);
		r2rmlWriter.handleNamespace(XSD.PREFIX, XSD.NAMESPACE);
		for (var s : model) {
			r2rmlWriter.handleStatement(s);
		}
		r2rmlWriter.endRDF();
	}

}
