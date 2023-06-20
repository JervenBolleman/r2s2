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

import static swiss.sib.swissprot.r2s2.JdbcUtil.openByJdbc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.loading.steps.IntroduceGraphEnum;
import swiss.sib.swissprot.r2s2.loading.steps.IntroduceHostEnums;
import swiss.sib.swissprot.r2s2.loading.steps.IntroduceIndexes;
import swiss.sib.swissprot.r2s2.loading.steps.IntroduceIriSchemeEnum;
import swiss.sib.swissprot.r2s2.loading.steps.OptimizeForR2RML;
import swiss.sib.swissprot.r2s2.loading.steps.ParseIntoSOGTables;
import swiss.sib.swissprot.r2s2.loading.steps.ReOptimizeForR2RML;
import swiss.sib.swissprot.r2s2.loading.steps.PoorMansVacuum;
import swiss.sib.swissprot.r2s2.optimization.IntroduceVirtualColumns;
import swiss.sib.swissprot.r2s2.optimization.TableMergingConcurence;
import swiss.sib.swissprot.r2s2.r2rml.R2RMLFromTables;
import swiss.sib.swissprot.r2s2.r2rml.TableDescriptionAsRdf;
import swiss.sib.swissprot.r2s2.sql.Table;

public class Loader {

	public static final Compression COMPRESSION = Compression.LZ4;
	private static final Logger logger = LoggerFactory.getLogger(Loader.class);
	static final char fieldSep = '\t';

	public static final long NOT_FOUND = -404;
	private final File dbFile;
	private final String jdbc;
	private final TemporaryIriIdMap predicatesInOrderOfSeen = new TemporaryIriIdMap();

	private final Map<String, String> namespaces = new ConcurrentHashMap<>();
	private volatile TemporaryIriIdMap temporaryGraphIdMap = new TemporaryIriIdMap();

	private final int step;
	private List<Table> tables = null;
	private List<String> lines;

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

	public Loader(File duckDbFile, int step, List<String> lines) {
		this(duckDbFile, step, lines, "jdbc:duckdb:" + duckDbFile.getAbsolutePath());
	}

	public Loader(File fileAsPattern, int step, List<String> lines, String driver) {
		super();
		this.dbFile = fileAsPattern;
		this.step = step;
		this.lines = lines;
		this.jdbc = driver;
		if (!fileAsPattern.exists()) {
			fileAsPattern.getParentFile().mkdirs();
		}
		namespaces.putIfAbsent("up", "http://purl.uniprot.org/core/");
		namespaces.putIfAbsent(RDF.PREFIX, RDF.NAMESPACE);
		namespaces.putIfAbsent(RDFS.PREFIX, RDFS.NAMESPACE);
		namespaces.putIfAbsent(XSD.PREFIX, XSD.NAMESPACE);

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
//		try {
//			Class.forName("org.duckdb.DuckDBDriver");
//		} catch (ClassNotFoundException e1) {
//			throw new IllegalStateException(e1);
//		}
		parse(directoryToWriteToo, lines, step);
	}

	static Loader parse(File directoryToWriteToo, List<String> lines, int step) throws SQLException, IOException {

		Loader wo = new Loader(directoryToWriteToo, step, lines);
		wo.parse();

		return wo;
	}

	public String connectionString() {
		if (jdbc.startsWith("jdbc:duckdb:")){
			return "jdbc:duckdb:"+ dbFile.getAbsolutePath();
		} else {
			return jdbc;
		}
	}

	public File descriptionPath() {
		final File p = dbFile.getParentFile();
		final String fn = dbFile.getName();
		return new File(p, fn + "-description.ttl");
	}

	public File r2rmlPath() {
		final File p = dbFile.getParentFile();
		final String fn = dbFile.getName();
		return new File(p, fn + "-r2rml.ttl");
	}

	private static final List<Consumer<Loader>> STEPS = List.of(Loader::parseOrReloadState,
			l -> new IntroduceGraphEnum(l.connectionString(), l.tables, l.temporaryGraphIdMap).run(),
			l -> l.tables = new OptimizeForR2RML(l.connectionString(), l.tables, l.namespaces).run(), Loader::writeR2RML,
			l -> l.tables = new TableMergingConcurence(l.connectionString(), l.tables).run(), Loader::writeR2RML,
			l -> l.tables = new ReOptimizeForR2RML(l.connectionString(), l.tables, l.namespaces).run(), Loader::writeR2RML,
			l -> new IntroduceIriSchemeEnum(l.connectionString(), l.tables).run(),
			l -> new IntroduceHostEnums(l.connectionString(), l.tables).run(),
			l -> new IntroduceIndexes(l.connectionString(), l.tables).run(),
			l -> new PoorMansVacuum(l.connectionString(), l.dbFile).run());

	public static void introduceVirtualColumns(Loader l) {
		for (Table t : l.tables) {
			try (Connection conn = openByJdbc(l.connectionString())) {
				IntroduceVirtualColumns.optimize(conn, t);
			} catch (SQLException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	public static void parseOrReloadState(Loader l) {
		try {
			logger.info("Testing if " + l.descriptionPath() + " exists");
			if (l.descriptionPath().exists()) {
				l.tables = TableDescriptionAsRdf.read(l.descriptionPath());
			} else {
				l.tables = new ParseIntoSOGTables(l.connectionString(), l.lines, l.predicatesInOrderOfSeen,
						l.temporaryGraphIdMap, l.namespaces).run();
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public static void writeR2RML(Loader l) {
		try {
			R2RMLFromTables.write(l.tables, l.r2rmlPath());
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public void parse() throws IOException, SQLException {
		if (step == 0) {
			logger.info("Starting all steps ");
			Instant start = Instant.now();
			for (int i = 0; i < STEPS.size(); i++) {
				runStep(i);
			}
			logger.info("Finished all steps in " + Duration.between(start, Instant.now()));
		} else {
			tables = TableDescriptionAsRdf.read(descriptionPath());
			runStep(step);
		}
	}

	public void runStep(int i) throws IOException {
		logger.info("Starting step " + (i));
		Instant start = Instant.now();
		STEPS.get(i).accept(this);
		logger.info("Finished step " + (i) + " in " + Duration.between(start, Instant.now()));
		TableDescriptionAsRdf.write(tables, descriptionPath());
	}

	public enum Kind {
		BNODE(), IRI(), LITERAL(), TRIPLE();

		Kind() {
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

		public static Kind fromLabel(String label) {
			switch (label) {
			case "triple":
				return TRIPLE;
			case "iri":
				return IRI;
			case "bnode":
				return BNODE;
			case "literal":
				return LITERAL;
			default:
				throw new IllegalStateException();
			}
		}
	}

	public List<Table> tables() {
		return tables;
	}
}
