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

import static swiss.sib.swissprot.r2s2.loading.ExternalProcessHelper.waitForProcessToBeDone;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.duckdb.DuckDBConnection;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.XMLParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.loading.LoadIntoTable.TargetKey;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap.TempIriId;
import swiss.sib.swissprot.r2s2.sql.Table;

public class Loader implements AutoCloseable {

	public static final Compression COMPRESSION = Compression.LZ4;
	private static final Logger logger = LoggerFactory.getLogger(Loader.class);
	static final char fieldSep = '\t';

	private static SimpleValueFactory vf = SimpleValueFactory.getInstance();

	private static final AtomicLong BNODE_ID_NORMALIZER = new AtomicLong();
	public static final long NOT_FOUND = -404;
	private final File directoryToWriteToo;
	private final TemporaryIriIdMap predicatesInOrderOfSeen = new TemporaryIriIdMap();
	private final Map<Integer, PredicateDirectoryWriter> predicatesDirectories = new ConcurrentHashMap<>();
	private volatile TemporaryIriIdMap temporaryGraphIdMap = new TemporaryIriIdMap();

	private final ExecutorService exec = Executors.newCachedThreadPool();
	/**
	 * Try to select a reasonable number of concurrent parse threads to actually
	 * run.
	 */
	private final Semaphore parsePresureLimit;
	/**
	 * Lock to protect the maps predicateInOrderOfSeen and predcateDirectories.
	 */
	private final Lock predicateSeenLock = new ReentrantLock();
	private final int step;

	/**
	 * Error exit codes.
	 */
	public static enum Failures {
		UNKOWN_FORMAT(1), GENERIC_RDF_PARSE_IO_ERROR(2), CUT_SORT_UNIQ_IO(4), GENERIC_RDF_PARSE_ERROR(5),
		TO_LOAD_FILE_NOT_CORRECT(7), NOT_DONE_YET(8), NO_GRAPH(9), GRAPH_ID_NOT_IRI(10);

		private final int exitCode;

		Failures(int i) {
			this.exitCode = i;
		}

		public void exit() {
			System.exit(exitCode);
		}
	}

	public Loader(File directoryToWriteToo) throws IOException, SQLException {
		this(directoryToWriteToo, 0);
	}

	public Loader(File directoryToWriteToo, int step) throws IOException, SQLException {
		super();
		this.directoryToWriteToo = directoryToWriteToo;
		this.step = step;
		if (!directoryToWriteToo.exists()) {
			directoryToWriteToo.mkdirs();
		}
		int procs = Runtime.getRuntime().availableProcessors();
		int estimateParsingProcessors = estimateParsingProcessors(procs);
		parsePresureLimit = new Semaphore(estimateParsingProcessors);
		logger.info("Running " + estimateParsingProcessors + " loaders ");
	}

	private int estimateParsingProcessors(int procs) {
		return Math.max(1, procs / 2);
	}

	public static void main(String args[]) throws IOException, SQLException {
		String fileDescribedToLoad = args[0];
		File directoryToWriteToo = new File(args[1]);
		Path path = Paths.get(fileDescribedToLoad);
		List<String> lines = Files.readAllLines(path);
		int step = 0;
		if (args.length >= 3) {
			step = Integer.parseInt(args[2]);
		}
		try (Connection conn_rw = DriverManager.getConnection("jdbc:duckdb:" + directoryToWriteToo.getAbsolutePath());
				Loader wo = new Loader(directoryToWriteToo, step)) {
			wo.parse(lines, conn_rw);
		} catch (IOException e) {
			logger.error("io", e);
		}
	}

	private Set<Table> parseFilesIntoPerPredicateType(List<String> lines, List<Future<?>> toRun, CountDownLatch latch,
			Connection conn_rw) throws SQLException {
		// We shuffle to increase the likelihood different kind of files are processed
		// at the same time.
		// files that are different often have different sets of predicates.
		Collections.shuffle(lines);

		for (String line : lines) {
			String[] fileGraph = line.split("\t");
			try {
				IRI graph = vf.createIRI(fileGraph[1]);
				String fileName = fileGraph[0];
				Optional<RDFFormat> parserFormatForFileName = Rio.getParserFormatForFileName(fileName);
				if (!parserFormatForFileName.isPresent()) {
					logger.error("Starting parsing of " + fileName + " failed because we can't guess format");
					Failures.UNKOWN_FORMAT.exit();
				} else {
					logger.info("Submitting parsing of " + fileName + " at " + Instant.now() + " with format "
							+ parserFormatForFileName.get());
					toRun.add(
							exec.submit(() -> parseInThread(latch, graph, fileName, parserFormatForFileName, conn_rw)));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				logger.error("Error in to load file at line, do you have filename tab graph iri: " + line);
				Failures.TO_LOAD_FILE_NOT_CORRECT.exit();
			}

		}
		WAIT: try {
			latch.await();
		} catch (InterruptedException e) {
			Thread.interrupted();
			break WAIT;
		}
		Stream<LoadIntoTable> flatMap = predicatesDirectories.values().stream().map(PredicateDirectoryWriter::getTargets)
				.flatMap(Collection::stream);
		return flatMap.distinct().map(LoadIntoTable::table).collect(Collectors.toSet());
	}

	private void parseInThread(CountDownLatch latch, IRI graph, String fileName,
			Optional<RDFFormat> parserFormatForFileName, Connection conn_rw) {
		try {
			parsePresureLimit.acquireUninterruptibly();
			parse(this, graph, fileName, parserFormatForFileName, conn_rw);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			Failures.GENERIC_RDF_PARSE_IO_ERROR.exit();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Failures.GENERIC_RDF_PARSE_ERROR.exit();
		} finally {
			parsePresureLimit.release();
		}
		latch.countDown();
	}

	private static void parse(Loader wo, IRI graph, String fileName, Optional<RDFFormat> parserFormatForFileName,
			Connection conn_rw) throws IOException {
		RDFParser parser = Rio.createParser(parserFormatForFileName.get());
		ParserConfig pc = parser.getParserConfig();
		pc.set(XMLParserSettings.FAIL_ON_DUPLICATE_RDF_ID, false);
		pc.set(XMLParserSettings.FAIL_ON_INVALID_QNAME, false);
		pc.set(XMLParserSettings.FAIL_ON_INVALID_NCNAME, false);
		pc.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
		// TODO support rdf-star.
		pc.set(BasicParserSettings.PROCESS_ENCODED_RDF_STAR, false);
		pc.setNonFatalErrors(Set.of(XMLParserSettings.FAIL_ON_DUPLICATE_RDF_ID));
		parser.setValueFactory(SimpleValueFactory.getInstance());
		try {
			Instant start = Instant.now();
			logger.info("Starting parsing of " + fileName + " at " + start);
			parser.setRDFHandler(wo.newHandler(graph, conn_rw));
			if (fileName.endsWith(".gz")) {
				Process cat = Compression.GZIP.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else if (fileName.endsWith(".bz2")) {
				Process cat = Compression.BZIP2.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else if (fileName.endsWith(".xz")) {
				Process cat = Compression.XZ.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else if (fileName.endsWith(".lz4")) {
				Process cat = Compression.LZ4.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else if (fileName.endsWith(".zstd") || fileName.endsWith(".zst")) {
				Process cat = Compression.ZSTD.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else {
				Process cat = Compression.NONE.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			}
			Instant end = Instant.now();
			logger.info("Finished parsing of " + fileName + " which took" + Duration.between(start, end) + "at " + end);
		} catch (RDF4JException e) {
			logger.error(e.getMessage() + " for " + fileName);
		} catch (RuntimeException e) {
			e.printStackTrace();
			logger.error(e.getMessage() + " for " + fileName);
		}
	}

	private static void parseWithInputViaCat(IRI graph, RDFParser parser, Process cat) throws IOException {

		try (InputStream gis = cat.getInputStream(); InputStream bis = new BufferedInputStream(gis, 128 * 1024)) {
			parser.parse(bis, graph.stringValue());
		}
		waitForProcessToBeDone(cat);
	}

	private RDFHandler newHandler(IRI graph, Connection conn_rw) {
		return new Handler(temporaryGraphIdMap.temporaryIriId(graph), conn_rw);
	}

	public void parse(List<String> lines, Connection conn_rw) throws IOException, SQLException {

		if (step <= 0) {
			stepOne(lines, conn_rw);
		}
	}

	private void stepOne(List<String> lines, Connection conn_rw) throws IOException, SQLException {
		Instant start = Instant.now();
		logger.info("Starting step 1 parsing files into temporary sorted ones");
		List<Future<SQLException>> closers = new ArrayList<>();
		List<Future<?>> toRun = new ArrayList<>();
		CountDownLatch latch = new CountDownLatch(lines.size());
		parseFilesIntoPerPredicateType(lines, toRun, latch, conn_rw);
		writeOutPredicates(closers, conn_rw);
		temporaryGraphIdMap.toDisk(directoryToWriteToo);
		logger.info("step 1 took " + Duration.between(start, Instant.now()));
	}

	private void writeOutPredicates(List<Future<SQLException>> closers, Connection conn_rw)
			throws IOException, SQLException {
		try (Connection conn_w_pred = ((DuckDBConnection) conn_rw).duplicate()) {
			try (java.sql.Statement ct = conn_w_pred.createStatement()) {
				ct.execute("CREATE OR REPLACE TABLE predicates (id INT PRIMARY KEY, iri VARCHAR)");
			}
			try (java.sql.Statement cs = conn_w_pred.createStatement()) {
				cs.execute("CREATE SEQUENCE predicate_ids START 1");
			}

			for (IRI predicateI : predicatesInOrderOfSeen.iris()) {
				try (PreparedStatement prepareStatement = conn_w_pred
						.prepareStatement("INSERT INTO predicates VALUES (nextval('predicate_ids'), ?);")) {
					String predicateS = predicateI.stringValue();
					prepareStatement.setString(1, predicateS);
					prepareStatement.execute();
				}
			}
		}
		for (TempIriId predicateI : predicatesInOrderOfSeen.iris()) {
			closers.add(exec.submit(() -> closeSingle(predicateI)));
		}

		ExternalProcessHelper.waitForFutures(closers);
	}

	private SQLException closeSingle(TempIriId predicate) {
		try {
			parsePresureLimit.acquireUninterruptibly();
			predicatesDirectories.get(predicate.id()).close();
		} catch (SQLException e) {
			return e;
		} finally {
			parsePresureLimit.release();
		}
		return null;
	}

	private LoadIntoTable writeStatement(File directoryToWriteToo, TemporaryIriIdMap predicatesInOrderOfSeen,
			Map<Integer, PredicateDirectoryWriter> predicateDirectories, Statement next, LoadIntoTable previous,
			Connection conn_rw) throws IOException, SQLException {
		PredicateDirectoryWriter predicateDirectoryWriter;
		if (previous != null && previous.testForAcceptance(next)) {
			previous.write(next);
			return previous;
		} else {
			TempIriId predicate = predicatesInOrderOfSeen.temporaryIriId(next.getPredicate());

			predicateDirectoryWriter = predicateDirectories.get(predicate.id());
			if (predicateDirectoryWriter == null) {
				predicateDirectoryWriter = addNewPredicateWriter(directoryToWriteToo, predicatesInOrderOfSeen,
						predicateDirectories, predicate, conn_rw);
			}
			return predicateDirectoryWriter.write(next);
		}
	}

	private PredicateDirectoryWriter addNewPredicateWriter(File directoryToWriteToo,
			TemporaryIriIdMap predicatesInOrderOfSeen, Map<Integer, PredicateDirectoryWriter> predicateDirectories,
			TempIriId predicate, Connection conn_rw) throws IOException, SQLException {
		PredicateDirectoryWriter predicateDirectoryWriter;
		try {
			predicateSeenLock.lock();

			int tempPredicateId = predicate.id();
			if (!predicateDirectories.containsKey(tempPredicateId)) {
				predicateDirectoryWriter = createPredicateDirectoryWriter(directoryToWriteToo, predicateDirectories,
						predicate, temporaryGraphIdMap, conn_rw);
				predicateDirectories.put(tempPredicateId, predicateDirectoryWriter);
			} else {
				predicateDirectoryWriter = predicateDirectories.get(predicate);
			}

		} finally {
			predicateSeenLock.unlock();
		}
		return predicateDirectoryWriter;
	}

	private PredicateDirectoryWriter createPredicateDirectoryWriter(File directoryToWriteToo,
			Map<Integer, PredicateDirectoryWriter> predicatesInOrderOfSeen, TempIriId predicate,
			TemporaryIriIdMap temporaryGraphIdMap, Connection conn_rw) throws IOException, SQLException {

		PredicateDirectoryWriter predicateDirectoryWriter = new PredicateDirectoryWriter(conn_rw, temporaryGraphIdMap,
				exec, predicate);
		return predicateDirectoryWriter;
	}

	public enum Kind {
		BNODE(0), IRI(1), LITERAL(2), TRIPLE(3);

		private final int sortOrder;

		Kind(int i) {
			this.sortOrder = i;
		}

		public static Kind of(Value val) {
			if (val.isIRI())
				return IRI;
			else if (val.isBNode())
				return BNODE;
			else if (val.isTriple())
				return TRIPLE;
			else
				return LITERAL;
		}

		public String label() {
			switch (this) {
			case TRIPLE:
				return "triple";
			case IRI:
				return "iri";
			case BNODE:
				return "bnode";
			case LITERAL:
				return "literal";
			default:
				throw new IllegalStateException();
			}
		}
	}

	static class PredicateDirectoryWriter implements AutoCloseable {
		private final Map<LoadIntoTable.TargetKey, LoadIntoTable> targets = new ConcurrentHashMap<>();

		public Collection<LoadIntoTable> getTargets() {
			return targets.values();
		}

		private final TemporaryIriIdMap tempraphIdMap;
		private final ExecutorService exec;
		private final Lock lock = new ReentrantLock();
		private final TempIriId predicate;
		private final Connection conn_rw;

		private PredicateDirectoryWriter(Connection conn_rw, TemporaryIriIdMap temporaryGraphIdMap,
				ExecutorService exec, TempIriId predicate) throws IOException, SQLException {
			this.conn_rw = conn_rw;
			this.tempraphIdMap = temporaryGraphIdMap;
			this.exec = exec;
			this.predicate = predicate;
		}

		/**
		 * Warning accessed from multiple threads.
		 *
		 * @param statement to write
		 * @throws IOException
		 * @throws SQLException
		 */
		private LoadIntoTable write(Statement statement) throws IOException, SQLException {
			TargetKey key = LoadIntoTable.key(statement);
			LoadIntoTable findAny = targets.get(key);
			if (findAny != null) {
				findAny.write(statement);
			} else {
				try {
					lock.lock();
					findAny = targets.get(key);
					if (findAny != null)
						findAny.write(statement);
					else {
						findAny = new LoadIntoTable(statement, conn_rw, tempraphIdMap, predicate);
						targets.put(key, findAny);
						findAny.write(statement);
					}
				} finally {
					lock.unlock();
				}
			}
			return findAny;
		}

		@Override
		public void close() throws SQLException {
			for (LoadIntoTable writer : targets.values()) {
				writer.close();
			}
		}

		public IRI getPredicate() {
			return predicate;
		}
	}

	private class Handler implements RDFHandler {
		private final Map<String, Long> bnodeMap = new HashMap<>();
		private final IRI graph;
		private LoadIntoTable previous = null;
		private final Connection conn;

		public Handler(IRI graph, Connection conn) {
			super();
			this.graph = graph;
			this.conn = conn;
		}

		@Override
		public void startRDF() throws RDFHandlerException {
			// TODO Auto-generated method stub

		}

		@Override
		public void endRDF() throws RDFHandlerException {
		}

		@Override
		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
			// TODO Auto-generated method stub

		}

		@Override
		public void handleStatement(Statement next) throws RDFHandlerException {
			if (next.getContext() == null) {
				next = vf.createStatement(next.getSubject(), next.getPredicate(), next.getObject(), graph);
			}
			if (next.getObject() instanceof BNode) {
				BNode bo = bnodeToReadOnlyBnode((BNode) next.getObject());
				next = vf.createStatement(next.getSubject(), next.getPredicate(), bo, next.getContext());
			}
			if (next.getSubject() instanceof BNode) {
				BNode bo = bnodeToReadOnlyBnode((BNode) next.getSubject());
				next = vf.createStatement(bo, next.getPredicate(), next.getObject(), next.getContext());
			}
			try {
				previous = writeStatement(directoryToWriteToo, predicatesInOrderOfSeen, predicatesDirectories, next,
						previous, conn);
			} catch (IOException | SQLException e) {
				logger.error("IO:", e);
				throw new RDFHandlerException("Failure passing data on", e);
			}

		}

		private BNode bnodeToReadOnlyBnode(BNode bo) {
			if (bnodeMap.containsKey(bo.getID())) {
				bo = new LoaderBlankNode(bnodeMap.get(bo.getID()));
			} else {
				long bnodeId = BNODE_ID_NORMALIZER.incrementAndGet();
				bnodeMap.put(bo.getID(), bnodeId);
				bo = new LoaderBlankNode(bnodeId);
			}
			return bo;
		}

		@Override
		public void handleComment(String comment) throws RDFHandlerException {
			// TODO Auto-generated method stub

		}
	}

	@Override
	public void close() {
		exec.shutdown();
	}
}
