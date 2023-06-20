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
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import swiss.sib.swissprot.r2s2.sql.GroupOfColumns;
import swiss.sib.swissprot.r2s2.sql.Naming;
import swiss.sib.swissprot.r2s2.sql.PredicateMap;
import swiss.sib.swissprot.r2s2.sql.Table;

public final class LoadIntoTable implements AutoCloseable {

	private static final int FLUSH_EVERY_X = 1 * 1024 * 1024;
	private static final Logger logger = LoggerFactory.getLogger(LoadIntoTable.class);
	private final Kind subjectKind;
	private final Kind objectKind;
	private final IRI datatype;
	private final String lang;
	private final TemporaryIriIdMap tgid;

	private volatile boolean closed = false;
	private final IRI predicate;
	private final Table table;
	private final Inserter inserter;
	private volatile int c = 1;
	private final Lock lock = new ReentrantLock();
	private final Connection conn;

	private interface Inserter {
		public void add(String[] subjParts, Resource subj, String[] objParts, Value obj, int tempGraphId)
				throws SQLException;

		public void close() throws SQLException;
	}

	private record JdbcInserter(PreparedStatement stat) implements Inserter {
		public JdbcInserter(Connection conn, String name, GroupOfColumns subjectColumns, GroupOfColumns objectColumns)
				throws SQLException {
			this(conn.prepareStatement("insert into " + name + '('
					+ Stream.concat(subjectColumns.columns().stream(), objectColumns.columns().stream())
							.map(Column::name).collect(Collectors.joining(", "))
					+ ") values (" + Stream.concat(subjectColumns.columns().stream(), objectColumns.columns().stream())
							.map(c -> "?").collect(Collectors.joining(", "))
					+ ")"));
		}

		public void add(String[] subjParts, Resource subj, String[] objParts, Value obj, int tempGraphId)
				throws SQLException {
			int offset = add(subjParts, subj, stat, 0);
			offset = add(objParts, obj, stat, offset);
			stat.setInt(++offset, tempGraphId);
			stat.executeUpdate();
		}

		public void close() throws SQLException {
			stat.close();
		}

		private int add(String[] parseO, Value v, PreparedStatement stat, int index) throws SQLException {
			if (v.isBNode()) {
				long i = ((LoaderBlankNode) v).id();
				stat.setLong(++index, i);
			} else {
				for (int i = 0; i < parseO.length; i++) {
					stat.setString(++index, parseO[i]);
				}
			}
			return index;
		}
	}

	private class DuckDbInserter implements Inserter {
		private final DuckDBAppender appender;
		private final DuckDBConnection conn;

		public DuckDbInserter(DuckDBConnection conn) throws SQLException {
			super();
			this.conn = conn;
			this.appender = conn.createAppender("", table.name());
		}

		public void add(String[] subjParts, Resource subj, String[] objParts, Value obj, int tempGraphId)
				throws SQLException {
			appender.beginRow();
			add(subjParts, subj, appender);
			add(objParts, obj, appender);
			appender.append(tempGraphId);
			appender.endRow();
			if (c % FLUSH_EVERY_X == 0) {
				appender.flush();
				logger.info("Flushed " + table.name() + " now has " + c + " rows");
			}
			c++;
		}

		public void close() throws SQLException {
			this.appender.close();
			this.conn.close();
		}

		private void add(String[] parseO, Value subjectS, DuckDBAppender appender) throws SQLException {
			if (subjectS.isBNode()) {
				long i = ((LoaderBlankNode) subjectS).id();
				appender.append(i);
			} else {
				for (int i = 0; i < parseO.length; i++) {
					appender.append(parseO[i]);
				}
			}
		}
	}

	public LoadIntoTable(Statement template, Connection masterConn, TemporaryIriIdMap tgid, TempIriId predicate,
			Map<String, String> namespaces) throws IOException, SQLException {

		this.conn = masterConn;
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
		GroupOfColumns subjectColumns = GroupOfColumns.from(subjectKind, lang, datatype, "subject_", namespaces,
				predicate);
		GroupOfColumns objectColumns = GroupOfColumns.from(objectKind, lang, datatype, "object_", namespaces,
				predicate);
		Column objectGraphColumn = GroupOfColumns.graphColumn(objectKind, lang, datatype, "object_", namespaces,
				predicate);
		objectColumns.columns().add(objectGraphColumn);
		final String tableName = tableName(predicate, namespaces, subjectKind, objectKind, lang, datatype);
		this.table = makeTable(predicate, subjectColumns, objectColumns, tableName);
		if (masterConn instanceof DuckDBConnection) {
			this.inserter = new DuckDbInserter((DuckDBConnection) ((DuckDBConnection) masterConn).duplicate());
		} else {
			this.inserter = new JdbcInserter(masterConn, this.table.name(), subjectColumns, objectColumns);
		}

	}

	public Table makeTable(TempIriId predicate, GroupOfColumns subjectColumns, GroupOfColumns objectColumns,
			final String tableName) throws SQLException {
		Table table;
		try {
			if (tableName != null) {
				PredicateMap pm = new PredicateMap(predicate, objectColumns, objectKind, lang, datatype);
				table = new Table(tableName, subjectColumns, subjectKind, List.of(pm));
			} else
				table = new Table(predicate, subjectColumns, subjectKind, objectColumns, objectKind, lang, datatype);
			table.create(conn);
		} catch (SQLException e) {
			// Can happen if the table name is not valid.
			table = new Table(predicate, subjectColumns, subjectKind, objectColumns, objectKind, lang, datatype);
			table.create(conn);
		}
		return table;
	}

	private static String tableName(TempIriId predicate, Map<String, String> namespaces, Kind subjectKind,
			Kind objectKind2, String lang2, IRI datatype2) {

		final String preName = Naming.iriToSqlNamePart(namespaces, predicate);
		if (preName != null) {
			return Table.generateName(preName, subjectKind, objectKind2, lang2, datatype2);
		} else {
			return null;
		}
	}

	@Override
	public void close() throws SQLException {
		if (!closed) {
			this.inserter.close();
		}
		logger.info("Closed " + table.name() + " now has " + c + " rows");
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
			final String[] parseS = parse(subjectS);
			final String[] parseO = parse(objectS);
			lock.lock();
			inserter.add(parseS, subjectS, parseO, objectS, tempGraphId);
		} finally {
			lock.unlock();
		}
	}

	private String[] parse(Value v) {
		if (v.isIRI()) {
			String[] r = new String[9];
			String i = v.stringValue();
			try {
				final URI u = new URI(i);
//				public static final String SCHEME = "_scheme";
				r[0] = u.getScheme();
//				public static final String SCHEME_SPECIFIC_PART = "_scheme_specific_part";
//				public static final String AUTHORITY = "_authority";
				if (u.getHost() == null) {
					r[1] = u.getSchemeSpecificPart();
					r[2] = u.getAuthority();
				} else {
//					stat.setString(++index, null);
//					stat.setString(++index, null);
				}
//				public static final String USER_INFO = "_user-info";
				r[3] = u.getUserInfo();
//				public static final String HOST = "_host";
				r[4] = u.getHost();
				if (u.getPort() >= 0) {
					r[5] = String.valueOf(u.getPort());
				}
//				public static final String PATH = "_path";
				r[6] = u.getPath();
//				public static final String QUERY = "_query";
				r[7] = u.getQuery();
//				public static final String FRAGMENT = "_fragment";
				r[8] = u.getFragment();
			} catch (URISyntaxException e) {
				throw new IllegalStateException(e);
			}
			return r;
		} else if (v.isBNode()) {
//			long i = ((LoaderBlankNode) v).id();
			return null;
		} else if (v.isLiteral()) {
			String[] r = new String[2];
			Literal l = (Literal) v;
			if (l.getLanguage().isPresent()) {
				r[0] = l.getLanguage().get();
			} else {
				IRI datatype = l.getDatatype();
				r[0] = datatype.stringValue();
			}
			r[1] = l.stringValue();
			return r;
		}
		throw new IllegalStateException("Unknown value type:" + v);
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
