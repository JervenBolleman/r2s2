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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;

public class TemporaryIriIdMap {
	private final Map<TempIriId, Integer> graphIriOrder = new ConcurrentHashMap<>();
	private final AtomicInteger ids = new AtomicInteger(0);
	private final Lock lock = new ReentrantLock();

	public int temporaryId(Resource r) {
		if (r instanceof IRI) {
			return temporaryIriId((IRI) r).id();
		} else {
			return (int) Loader.NOT_FOUND;
		}
	}

	public TempIriId temporaryIriId(IRI graphIri) {
		Integer got = graphIriOrder.get(graphIri);
		if (got == null) {
			try {
				lock.lock();
				if (!graphIriOrder.containsKey(graphIri)) {
					TempIriId temp = new TempIriId((IRI) graphIri, ids.getAndIncrement());
					graphIriOrder.put(temp, temp.id());
					return temp;
				}
				got = graphIriOrder.get(graphIri);
			} finally {
				lock.unlock();
			}
		}
		return new TempIriId((IRI) graphIri, got);
	}

	public Collection<TempIriId> iris() {
		return graphIriOrder.keySet();
	}
	
	/**
	 * This class reduces contention on the tempGraphId map. Or in practical terms
	 * saves a billion or so string hashCode operations etc.
	 */
	public static final class TempIriId implements IRI {

		private static final long serialVersionUID = 1L;
		private final IRI wrapped;
		private final int id;
		private final int hashcode;

		public TempIriId(IRI wrapped, int id) {
			super();
			this.wrapped = wrapped;
			this.id = id;
			this.hashcode = wrapped.hashCode();
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
			return hashcode;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (obj instanceof TempIriId) {
				boolean same = (id == ((TempIriId) obj).id);
				assert same == wrapped.equals(obj);
				return same;
			}
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

		@Override
		public String toString() {
			return wrapped.toString();
		}
	}
}
