package swiss.sib.swissprot.r2s2.loading.steps;

import static swiss.sib.swissprot.r2s2.DuckDBUtil.checkpoint;
import static swiss.sib.swissprot.r2s2.DuckDBUtil.open;
import static swiss.sib.swissprot.r2s2.loading.ExternalProcessHelper.waitForProcessToBeDone;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
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

import swiss.sib.swissprot.r2s2.loading.Compression;
import swiss.sib.swissprot.r2s2.loading.ExternalProcessHelper;
import swiss.sib.swissprot.r2s2.loading.LoadIntoTable;
import swiss.sib.swissprot.r2s2.loading.LoadIntoTable.TargetKey;
import swiss.sib.swissprot.r2s2.loading.Loader.Failures;
import swiss.sib.swissprot.r2s2.loading.LoaderBlankNode;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap.TempIriId;
import swiss.sib.swissprot.r2s2.sql.Table;

public class ParseIntoSOGTables {
	private static SimpleValueFactory vf = SimpleValueFactory.getInstance();

	private static final AtomicLong BNODE_ID_NORMALIZER = new AtomicLong();
	private final Map<Integer, PredicateDirectoryWriter> predicatesDirectories = new ConcurrentHashMap<>();
	private final String tempPath;
	private final List<String> lines;
	private final TemporaryIriIdMap predicatesInOrderOfSeen;
	private final TemporaryIriIdMap temporaryGraphIdMap;
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

	private final Map<String, String> namespaces;

	public ParseIntoSOGTables(String tempPath, List<String> lines, TemporaryIriIdMap predicatesInOrderOfSeen,
			TemporaryIriIdMap temporaryGraphIdMap, Map<String, String> namespaces) {
		this.tempPath = tempPath;
		this.lines = lines;
		this.predicatesInOrderOfSeen = predicatesInOrderOfSeen;
		this.temporaryGraphIdMap = temporaryGraphIdMap;
		this.namespaces = namespaces;
		int procs = Runtime.getRuntime().availableProcessors();
		int estimateParsingProcessors = estimateParsingProcessors(procs);
		parsePresureLimit = new Semaphore(estimateParsingProcessors);
		logger.info("Running " + estimateParsingProcessors + " loaders ");

	}

	private static final Logger logger = LoggerFactory.getLogger(ParseIntoSOGTables.class);

	public List<Table> run() throws IOException {
		try (Connection conn_rw = open(tempPath)) {
			Instant start = Instant.now();
			logger.info("Starting step parsing files into SOG tables, named by predicate");
			List<Future<SQLException>> closers = new ArrayList<>();
			List<Future<?>> toRun = new ArrayList<>();
			CountDownLatch latch = new CountDownLatch(lines.size());
			parseFilesIntoPerPredicateType(lines, toRun, latch, conn_rw);
			writeOutPredicates(closers, conn_rw);
			tempIriIdMapIntoTable(conn_rw, "graphs", temporaryGraphIdMap);
			logger.info("Parsing files into SOG tables took " + Duration.between(start, Instant.now()));
			checkpoint(conn_rw);
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}

		List<Table> l = new ArrayList<>();
		for (var me : predicatesDirectories.entrySet()) {
			PredicateDirectoryWriter value = me.getValue();
			for (var t : value.getTargets()) {
				l.add(t.table());
			}
		}
		return l;
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
		for (var p : predicatesDirectories.values()) {
			p.close();
		}
		Stream<LoadIntoTable> flatMap = predicatesDirectories.values().stream()
				.map(PredicateDirectoryWriter::getTargets).flatMap(Collection::stream);
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

	private void writeOutPredicates(List<Future<SQLException>> closers, Connection conn_rw)
			throws IOException, SQLException {
		tempIriIdMapIntoTable(conn_rw, "predicates", predicatesInOrderOfSeen);
		for (TempIriId predicateI : predicatesInOrderOfSeen.iris()) {
			closers.add(exec.submit(() -> closeSingle(predicateI)));
		}

		ExternalProcessHelper.waitForFutures(closers);
		exec.shutdown();
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

	private LoadIntoTable writeStatement(TemporaryIriIdMap predicatesInOrderOfSeen,
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
				predicateDirectoryWriter = addNewPredicateWriter(predicatesInOrderOfSeen,
						predicateDirectories, predicate, conn_rw);
			}
			return predicateDirectoryWriter.write(next);
		}
	}

	private PredicateDirectoryWriter addNewPredicateWriter(TemporaryIriIdMap predicatesInOrderOfSeen,
			Map<Integer, PredicateDirectoryWriter> predicateDirectories, TempIriId predicate, Connection conn_rw)
			throws IOException, SQLException {
		PredicateDirectoryWriter predicateDirectoryWriter;
		try {
			predicateSeenLock.lock();

			int tempPredicateId = predicate.id();
			if (!predicateDirectories.containsKey(tempPredicateId)) {
				predicateDirectoryWriter = createPredicateDirectoryWriter(predicateDirectories, predicate,
						temporaryGraphIdMap, conn_rw);
				predicateDirectories.put(tempPredicateId, predicateDirectoryWriter);
			} else {
				predicateDirectoryWriter = predicateDirectories.get(predicate.id());
			}

		} finally {
			predicateSeenLock.unlock();
		}
		return predicateDirectoryWriter;
	}

	private PredicateDirectoryWriter createPredicateDirectoryWriter(
			Map<Integer, PredicateDirectoryWriter> predicatesInOrderOfSeen, TempIriId predicate,
			TemporaryIriIdMap temporaryGraphIdMap, Connection conn_rw) throws IOException, SQLException {

		PredicateDirectoryWriter predicateDirectoryWriter = new PredicateDirectoryWriter(conn_rw, temporaryGraphIdMap,
				predicate, namespaces);
		return predicateDirectoryWriter;
	}

	static class PredicateDirectoryWriter implements AutoCloseable {
		private final Map<LoadIntoTable.TargetKey, LoadIntoTable> targets = new ConcurrentHashMap<>();

		public Collection<LoadIntoTable> getTargets() {
			return targets.values();
		}

		private final TemporaryIriIdMap tempraphIdMap;
		private final Lock lock = new ReentrantLock();
		private final TempIriId predicate;
		private final Connection conn_rw;
		private final Map<String, String> namespaces;

		private PredicateDirectoryWriter(Connection conn_rw, TemporaryIriIdMap temporaryGraphIdMap, TempIriId predicate,
				Map<String, String> namespaces) throws IOException, SQLException {
			this.conn_rw = conn_rw;
			this.tempraphIdMap = temporaryGraphIdMap;
			this.predicate = predicate;
			this.namespaces = namespaces;
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
						findAny = new LoadIntoTable(statement, conn_rw, tempraphIdMap, predicate, namespaces);
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
			try {
				lock.lock();
				for (LoadIntoTable writer : targets.values()) {
					writer.close();
				}
			} finally {
				lock.unlock();
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
			namespaces.putIfAbsent(prefix, uri);
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
				previous = writeStatement(predicatesInOrderOfSeen, predicatesDirectories, next,
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

	private RDFHandler newHandler(IRI graph, Connection conn_rw) {
		return new Handler(temporaryGraphIdMap.temporaryIriId(graph), conn_rw);
	}

	private int estimateParsingProcessors(int procs) {
		return Math.max(1, procs / 2);
	}

	private static void parse(ParseIntoSOGTables wo, IRI graph, String fileName,
			Optional<RDFFormat> parserFormatForFileName, Connection conn_rw) throws IOException {
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

	private void tempIriIdMapIntoTable(Connection conn_rw, String tableName, TemporaryIriIdMap m) throws SQLException {
		try (Connection conn_w_pred = ((DuckDBConnection) conn_rw).duplicate()) {
			try (java.sql.Statement ct = conn_w_pred.createStatement()) {
				ct.execute("CREATE OR REPLACE TABLE " + tableName + " (id INT PRIMARY KEY, iri VARCHAR)");
			}

			for (TempIriId predicateI : m.iris()) {
				try (PreparedStatement prepareStatement = conn_w_pred
						.prepareStatement("INSERT INTO " + tableName + " VALUES (?, ?);")) {
					String predicateS = predicateI.stringValue();
					prepareStatement.setInt(1, predicateI.id());
					logger.info("Writing into " + tableName + " id:" + predicateI.id() + " iri:" + predicateS);
					prepareStatement.setString(2, predicateS);
					prepareStatement.execute();
				}
			}
		}
	}
}
