/*******************************************************************************
 * Copyright (c) 2022 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package swiss.sib.swissprot.r2s2.loading;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap.TempIriId;
import swiss.sib.swissprot.r2s2.sql.Column;
import swiss.sib.swissprot.r2s2.sql.Columns;
import swiss.sib.swissprot.r2s2.sql.Datatypes;
import swiss.sib.swissprot.r2s2.sql.Naming;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;

final class LoadIntoTable implements AutoCloseable {

	private static final int FLUSH_EVERY_X = 1 * 1024 * 1024;
	private static final Logger logger = LoggerFactory.getLogger(LoadIntoTable.class);
	private final Kind subjectKind;
	private final Kind objectKind;
	private final IRI datatype;
	private final String lang;
	private final DuckDBConnection conn;
	private final TemporaryIriIdMap tgid;

	private volatile boolean closed = false;
	private final IRI predicate;
	private final Table table;
	private final DuckDBAppender appender;
	private volatile int c = 1;
	private final Lock lock = new ReentrantLock();

	LoadIntoTable(Statement template, Connection masterConn, TemporaryIriIdMap tgid, TempIriId predicate,
			Map<String, String> namespaces) throws IOException, SQLException {
		this.conn = (DuckDBConnection) ((DuckDBConnection) masterConn).duplicate();
		this.tgid = tgid;
		this.subjectKind = Kind.of(template.getSubject());
		this.objectKind = Kind.of(template.getObject());
		this.predicate = predicate;
		if (objectKind != Kind.LITERAL) {
			lang = null;
			datatype = null;
		} else {
			Literal lit = (Literal) template.getObject();
			lang = lit.getLanguage().orElse(null);
			datatype = lit.getDatatype();
		}
		Columns subjectColumns = Columns.from(subjectKind, lang, datatype, "subject_", namespaces,predicate);
		Columns objectColumns = Columns.from(objectKind, lang, datatype, "object_",namespaces, predicate);
		Column objectGraphColumn = Columns.graphColumn(objectKind, lang, datatype, "object_",namespaces, predicate);
		objectColumns.getColumns().add(objectGraphColumn);
		final String tableName = tableName(predicate, namespaces, subjectKind, objectKind, lang, datatype);
		this.table = makeTable(predicate, subjectColumns, objectColumns, tableName);

		this.appender = conn.createAppender("", table.name());
	}

	public Table makeTable(TempIriId predicate, Columns subjectColumns, Columns objectColumns, 
			final String tableName) throws SQLException {
		Table table;
		try {
			if (tableName != null) {
				PredicateMap pm = new PredicateMap(predicate, objectColumns, objectKind, lang, datatype);
				table = new Table(tableName, subjectColumns, subjectKind, List.of(pm));
			} else
				table = new Table(predicate, subjectColumns, subjectKind, objectColumns, objectKind,  lang,
						datatype);
			table.create(conn);
		} catch (SQLException e) {
			// Can happen if the table name is not valid.
			table = new Table(predicate, subjectColumns, subjectKind, objectColumns, objectKind, lang,
					datatype);
			table.create(conn);
		}
		return table;
	}

	private static String tableName(TempIriId predicate, Map<String, String> namespaces, Kind subjectKind, Kind objectKind2, String lang2,
			IRI datatype2) {
		
		final String preName = Naming.iriToSqlNamePart(namespaces, predicate);
		if(preName != null) {
			return Table.generateName(preName, subjectKind, objectKind2, lang2, datatype2);
		} else {
			return null;
		}
	}

	@Override
	public void close() throws SQLException {
		if (!closed) {
			this.appender.close();
			logger.info("Closed " + table.name() + " now has " + c + " rows");
			this.conn.close();
		}
		closed = true;
	}

	/**
	 * Test if the statement may be written by this target.
	 *
	 * @param statement
	 * @return true if it is accepted
	 */
	public boolean testForAcceptance(Statement statement) {
		if (predicate.equals(statement.getPredicate())) {
			if (Kind.of(statement.getSubject()) != this.subjectKind) {
				return false;
			} else if (Kind.of(statement.getObject()) != this.objectKind) {
				return false;
			} else if (this.objectKind == Kind.LITERAL) {
				Literal lit = (Literal) statement.getObject();
				String otherLang = lit.getLanguage().orElse(null);
				IRI otherDatatype = lit.getDatatype();
				boolean langEq = Objects.equals(this.lang, otherLang);
				boolean datatypeEq = Objects.equals(this.datatype, otherDatatype);
				return langEq && datatypeEq;
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Used for faster hashcoding and lookups.
	 *
	 * @param statement
	 * @return a new TargetKey that matches this statement
	 */
	public static TargetKey key(Statement statement) {
		Kind objectKind = Kind.of(statement.getObject());
		if (objectKind != Kind.LITERAL) {
			return new TargetKey(Kind.of(statement.getSubject()), objectKind, null, null);
		} else {
			Literal lit = (Literal) statement.getObject();
			String otherLang = lit.getLanguage().orElse(null);
			IRI otherDatatype = lit.getDatatype();
			return new TargetKey(Kind.of(statement.getSubject()), objectKind, otherLang, otherDatatype);
		}
	}

	public static record TargetKey(Kind subjectKind, Kind objectKind, String otherLang, IRI otherDatatype) {
	}

	public void write(Statement statement) throws SQLException {
		Resource subjectS = statement.getSubject();
		Value objectS = statement.getObject();
		assert statement.getPredicate().equals(predicate);
		if (statement.getContext() != null) {
			int tempGraphId = getTemporaryGraphId(statement);
			write(subjectS, objectS, tempGraphId);
		} else {
			Loader.Failures.NO_GRAPH.exit();
		}
	}

	private int getTemporaryGraphId(Statement statement) {
		int tempGraphId;
		Resource context = statement.getContext();
		if (context instanceof TempIriId t) {
			tempGraphId = t.id();
		} else {
			tempGraphId = tgid.temporaryId(context);
		}
		return tempGraphId;
	}

	private void write(Resource subjectS, Value objectS, int tempGraphId) throws SQLException {
		try {
			lock.lock();
			appender.beginRow();
			table.subject().add(subjectS, appender);
			table.objects().get(0).columns().add(objectS, tempGraphId, appender);
			appender.endRow();
			if (c % FLUSH_EVERY_X == 0) {
				appender.flush();
				logger.info("Flushed " + table.name() + " now has " + c + " rows");
			}
			c++;
		} finally {
			lock.unlock();
		}
	}

	public Kind subjectKind() {
		return subjectKind;
	}

	public Kind objectKind() {
		return objectKind;
	}

	public IRI datatype() {
		return datatype;
	}

	public String lang() {
		return lang;
	}

	public Table table() {
		return table;
	}
}
