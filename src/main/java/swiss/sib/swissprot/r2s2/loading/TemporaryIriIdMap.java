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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemporaryIriIdMap {
	private static final Logger logger = LoggerFactory.getLogger(TemporaryIriIdMap.class);
	private final Set<TempIriId> graphIriInOrder = Collections.synchronizedSet(new LinkedHashSet<>());
	private final Map<IRI, Integer> graphIriOrder = new ConcurrentHashMap<>();
	private final Map<Integer, IRI> iriOrderGraph = new ConcurrentHashMap<>();
	private final Lock lock = new ReentrantLock();

	public int temporaryId(Resource r) {
		if (r instanceof IRI) {
			return temporaryIriId((IRI) r).id();
		} else {
			return (int) Loader.NOT_FOUND;
		}
	}

	public TempIriId temporaryIriId(IRI graphIri) {
		if (graphIri instanceof IRI) {
			Integer got = graphIriOrder.get(graphIri);
			if (got == null) {
				try {
					lock.lock();
					if (!graphIriInOrder.contains(graphIri)) {
						int tempId = graphIriInOrder.size();
						TempIriId temp = new TempIriId((IRI) graphIri, tempId++);
						graphIriInOrder.add(temp);
						graphIriOrder.put(temp, tempId);
						iriOrderGraph.put(tempId, temp);
						return temp;
					}
					got = graphIriOrder.get(graphIri);
				} finally {
					lock.unlock();
				}
			}
			return new TempIriId((IRI) graphIri, got);
		}
		logger.error("recieved graph that is not an IRI" + graphIri);
		Loader.Failures.GRAPH_ID_NOT_IRI.exit();
		return new TempIriId(graphIri, (int) Loader.NOT_FOUND);
	}

	public Collection<TempIriId> iris() {
		return graphIriInOrder;
	}

	public IRI iriFromTempIriId(int id) {
		return iriOrderGraph.get(Integer.valueOf(id));
	}

	public Integer parseInt(String s) {
		return Integer.parseUnsignedInt(s, 16);
	}

	public void toDisk(File rootDir) throws IOException {
		Files.writeString(extracted(rootDir).toPath(),
				graphIriInOrder.stream().map(IRI::stringValue).collect(Collectors.joining("\n")),
				StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
	}

	public static TemporaryIriIdMap fromDisk(File rootDir) throws IOException {
		TemporaryIriIdMap r = new TemporaryIriIdMap();

		Path path = extracted(rootDir).toPath();
		if (Files.exists(path)) {
			try (Stream<String> lines = Files.lines(path)) {
				lines.map(s -> SimpleValueFactory.getInstance().createIRI(s)).forEach(r::temporaryIriId);
			}
		}
		return r;

	}

	private static File extracted(File rootDir) {
		return new File(rootDir.getParentFile(), "graphs");
	}
	
	/**
	 * This class reduces contention on the tempGraphId map. Or in practical terms
	 * saves a billion or so string hashCode operations etc.
	 */
	public static final class TempIriId implements IRI {

		private static final long serialVersionUID = 1L;
		private final IRI wrapped;
		private final int id;

		public TempIriId(IRI wrapped, int id) {
			super();
			this.wrapped = wrapped;
			this.id = id;
		}

		@Override
		public String stringValue() {
			return wrapped.stringValue();
		}

		@Override
		public String getNamespace() {
			return wrapped.getNamespace();
		}

		@Override
		public String getLocalName() {
			return wrapped.getLocalName();
		}

		@Override
		public int hashCode() {
			return wrapped.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (obj instanceof TempIriId)
				return id == ((TempIriId) obj).id;
			if (obj instanceof IRI other)
				return wrapped.equals(other);
			return false;
		}

		/**
		 * @return the temporary id associated with this IRI as a Graph
		 */
		public int id() {
			return id;
		}
	}
}
