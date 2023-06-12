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

import static swiss.sib.swissprot.r2s2.DuckDBUtil.checkpoint;
import static swiss.sib.swissprot.r2s2.DuckDBUtil.open;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.loading.steps.IntroduceGraphEnum;
import swiss.sib.swissprot.r2s2.loading.steps.IntroduceHostEnums;
import swiss.sib.swissprot.r2s2.loading.steps.IntroduceProtocolEnums;
import swiss.sib.swissprot.r2s2.loading.steps.OptimizeForR2RML;
import swiss.sib.swissprot.r2s2.loading.steps.ParseIntoSOGTables;
import swiss.sib.swissprot.r2s2.loading.steps.Vacuum;
import swiss.sib.swissprot.r2s2.optimization.TableMergingConcurence;
import swiss.sib.swissprot.r2s2.r2rml.R2RMLFromTables;
import swiss.sib.swissprot.r2s2.r2rml.TableDescriptionAsRdf;
import swiss.sib.swissprot.r2s2.sql.Table;

public class Loader {

	public static final Compression COMPRESSION = Compression.LZ4;
	private static final Logger logger = LoggerFactory.getLogger(Loader.class);
	static final char fieldSep = '\t';

	public static final long NOT_FOUND = -404;
	private final File directoryToWriteToo;
	private final TemporaryIriIdMap predicatesInOrderOfSeen = new TemporaryIriIdMap();

	private final Map<String, String> namespaces = new ConcurrentHashMap<>();
	private volatile TemporaryIriIdMap temporaryGraphIdMap = new TemporaryIriIdMap();

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
			directoryToWriteToo.getParentFile().mkdirs();
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
		try {
			Class.forName("org.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e1) {
			throw new IllegalStateException(e1);
		}
		parse(directoryToWriteToo, lines, step);
	}

	static Loader parse(File directoryToWriteToo, List<String> lines, int step) throws SQLException, IOException {

		Loader wo = new Loader(directoryToWriteToo, step);
		final String tempPath = tempPath(directoryToWriteToo);

		wo.parse(lines, tempPath);

		return wo;
	}

	public static String tempPath(File directoryToWriteToo) {
		return directoryToWriteToo.getAbsolutePath() + ".loading-tmp";
	}

	public static File descriptionPath(String dt) {
		final File p = new File(dt).getParentFile();
		final String fn = new File(dt).getName();
		return new File(p, fn + "-description.ttl");
	}

	public static File r2rmlPath(String dt) {
		final File p = new File(dt).getParentFile();
		final String fn = new File(dt).getName();
		return new File(p, fn + "-r2rml.ttl");
	}

	public void parse(List<String> lines, String tempPath) throws IOException, SQLException {
		List<Table> tables = null;
		if (step > 1) {
			tables = TableDescriptionAsRdf.read(descriptionPath(tempPath));;
		}
		if (step == 1 || step == 0) {
			logger.info("Starting step 1");
			tables = stepOne(lines, tempPath);
			TableDescriptionAsRdf.write(tables, descriptionPath(tempPath));
		}
		if (step == 2 || step == 0) {
			logger.info("Starting step 2");
			tables = stepTwo(tempPath, tables);
			TableDescriptionAsRdf.write(tables, descriptionPath(tempPath));
		}
		if (step == 3 || step == 0) {
			logger.info("Starting step 3: merging tables");
			tables = stepThree(tempPath, tables);
			TableDescriptionAsRdf.write(tables, descriptionPath(tempPath));
		}
		if (step == 4 || step == 0) {
			logger.info("Starting step 4, which is step 2 again");
			tables = stepTwo(tempPath, tables);
			TableDescriptionAsRdf.write(tables, descriptionPath(tempPath));
		}
		if (step == 5 || step == 0) {
			logger.info("Starting step 5: introducing enum types");
			stepFive(tempPath, tables);
		}
		if (step == 6 || step == 0) {
			logger.info("Starting step 6, a poor mans vacuum");
			stepSix(tempPath);
			TableDescriptionAsRdf.write(tables, descriptionPath(tempPath));
		}
	}

	public void stepFive(String tempPath, List<Table> tables) throws SQLException {
		new IntroduceProtocolEnums(tempPath, tables, temporaryGraphIdMap).run();
		new IntroduceHostEnums(tempPath, tables, temporaryGraphIdMap).run();
	}

	List<Table> stepOne(List<String> lines, String tempPath) throws IOException, SQLException {
		List<Table> tables = new ParseIntoSOGTables(tempPath, lines, predicatesInOrderOfSeen, temporaryGraphIdMap,
				namespaces).run();
		new IntroduceGraphEnum(tempPath, tables, temporaryGraphIdMap).run();
		return tables;
	}

	List<Table> stepTwo(String tempPath, List<Table> tables) throws SQLException, IOException {
		tables = new OptimizeForR2RML(tempPath, tables, namespaces).run();
		R2RMLFromTables.write(tables, r2rmlPath(tempPath));
		return tables;
	}

	List<Table> stepThree(String tempPath, List<Table> tables) throws SQLException {
		try (Connection conn = open(tempPath)) {
			tables = new TableMergingConcurence(conn, tables).run();
			checkpoint(conn);
		}
		return tables;
	}

	void stepSix(String tempPath) throws SQLException, IOException {
		new Vacuum(tempPath, directoryToWriteToo.getAbsolutePath()).run();
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
}
