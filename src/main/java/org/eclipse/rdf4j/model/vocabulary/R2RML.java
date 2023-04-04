package org.eclipse.rdf4j.model.vocabulary;

/*******************************************************************************
 * Copyright (c) 2023 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;

/**
 * Constants for R2RML primitives and for the R2RML namespace.
 *
 * @see <a href="https://www.w3.org/TR/r2rml/">R2RML: RDB to RDF Mapping
 *      Language</a>
 */

public class R2RML {
	/** http://www.w3.org/ns/r2rml# */
	public static final String NAMESPACE = "http://www.w3.org/ns/r2rml#";

	/**
	 * Recommended prefix for the R2RML namespace:"rr"
	 */
	public static final String PREFIX = "rr";

	/**
	 * An immutable {@link Namespace} constant that represents the RR namespace.
	 */
	public static final Namespace NS = Vocabularies.createNamespace(PREFIX, NAMESPACE);

	public static final IRI child;

	/**
	 * class is a keyword so we use clazz like it is common when doing css in java.
	 */
	public static final IRI clazz;
	public static final IRI column;
	public static final IRI datatype;
	public static final IRI constant;
	public static final IRI graph;
	public static final IRI graphMap;
	public static final IRI inverseExpression;
	public static final IRI joinCondition;
	public static final IRI language;
	public static final IRI logicalTable;
	public static final IRI object;
	public static final IRI objectMap;
	public static final IRI parent;
	public static final IRI parentTriplesMap;
	public static final IRI predicate;
	public static final IRI predicateMap;
	public static final IRI predicateObjectMap;
	public static final IRI sqlQuery;
	public static final IRI sqlVersion;
	public static final IRI subject;
	public static final IRI subjectMap;
	public static final IRI tableName;
	public static final IRI template;
	public static final IRI termType;
	public static final IRI defaultGraph;
	public static final IRI SQL2008;
	public static final IRI IRI;
	public static final IRI BlankNode;
	public static final IRI Literal;
	static {
		child = Vocabularies.createIRI(R2RML.NAMESPACE, "child");
		clazz = Vocabularies.createIRI(R2RML.NAMESPACE, "class");
		column = Vocabularies.createIRI(R2RML.NAMESPACE, "column");
		datatype = Vocabularies.createIRI(R2RML.NAMESPACE, "datatype");
		constant = Vocabularies.createIRI(R2RML.NAMESPACE, "constant");
		graph = Vocabularies.createIRI(R2RML.NAMESPACE, "graph");
		graphMap = Vocabularies.createIRI(R2RML.NAMESPACE, "graphMap");
		inverseExpression = Vocabularies.createIRI(R2RML.NAMESPACE, "inverseExpression");
		joinCondition = Vocabularies.createIRI(R2RML.NAMESPACE, "joinCondition");
		language = Vocabularies.createIRI(R2RML.NAMESPACE, "language");
		logicalTable = Vocabularies.createIRI(R2RML.NAMESPACE, "logicalTable");
		object = Vocabularies.createIRI(R2RML.NAMESPACE, "object");
		objectMap = Vocabularies.createIRI(R2RML.NAMESPACE, "objectMap");
		parent = Vocabularies.createIRI(R2RML.NAMESPACE, "parent");
		parentTriplesMap = Vocabularies.createIRI(R2RML.NAMESPACE, "parentTriplesMap");
		predicate = Vocabularies.createIRI(R2RML.NAMESPACE, "predicate");
		predicateMap = Vocabularies.createIRI(R2RML.NAMESPACE, "predicateMap");
		predicateObjectMap = Vocabularies.createIRI(R2RML.NAMESPACE, "predicateObjectMap");
		sqlQuery = Vocabularies.createIRI(R2RML.NAMESPACE, "sqlQuery");
		sqlVersion = Vocabularies.createIRI(R2RML.NAMESPACE, "sqlVersion");
		subject = Vocabularies.createIRI(R2RML.NAMESPACE, "subject");
		subjectMap = Vocabularies.createIRI(R2RML.NAMESPACE, "subjectMap");
		tableName = Vocabularies.createIRI(R2RML.NAMESPACE, "tableName");
		template = Vocabularies.createIRI(R2RML.NAMESPACE, "template");
		termType = Vocabularies.createIRI(R2RML.NAMESPACE, "termType");
		defaultGraph = Vocabularies.createIRI(R2RML.NAMESPACE, "defaultGraph");
		SQL2008 = Vocabularies.createIRI(R2RML.NAMESPACE, "SQL2008");
		IRI = Vocabularies.createIRI(R2RML.NAMESPACE, "IRI");
		BlankNode = Vocabularies.createIRI(R2RML.NAMESPACE, "BlankNode");
		Literal = Vocabularies.createIRI(R2RML.NAMESPACE, "Literal");
	}
}
