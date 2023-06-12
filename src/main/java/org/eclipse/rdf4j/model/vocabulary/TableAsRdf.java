package org.eclipse.rdf4j.model.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;

public class TableAsRdf {
	/** TODO */
	public static final String NAMESPACE = "http://www.example.org/table#";

	public static final String PREFIX = "table";

	public static final Namespace NS = Vocabularies.createNamespace(PREFIX, NAMESPACE);

	public static final IRI table;
	public static final IRI columns;
	public static final IRI column;
	public static final IRI virtualColumn;
	public static final IRI physicalColumn;

	public static final IRI subjectColumns;

	public static final IRI objectColumns;

	public static final IRI predicate;

	public static final IRI datatype;

	public static final IRI lang;

	public static final IRI kind;

	static {
		table = Vocabularies.createIRI(NAMESPACE, "table");
		columns = Vocabularies.createIRI(NAMESPACE, "columns");
		predicate = Vocabularies.createIRI(NAMESPACE, "predicate");
		kind = Vocabularies.createIRI(NAMESPACE, "kind");
		lang = Vocabularies.createIRI(NAMESPACE, "lang");
		datatype = Vocabularies.createIRI(NAMESPACE, "datatype");
		column = Vocabularies.createIRI(NAMESPACE, "Column");
		subjectColumns = Vocabularies.createIRI(NAMESPACE, "subjectColumns");
		objectColumns = Vocabularies.createIRI(NAMESPACE, "objectColumns");
		virtualColumn = Vocabularies.createIRI(NAMESPACE, "VirtualColumn");
		physicalColumn = Vocabularies.createIRI(NAMESPACE, "PhysicalColumn");
	}
}
