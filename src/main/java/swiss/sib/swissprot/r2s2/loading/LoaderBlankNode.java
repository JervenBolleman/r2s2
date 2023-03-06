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

import org.eclipse.rdf4j.model.BNode;

public class LoaderBlankNode implements BNode {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private final long id;

	public LoaderBlankNode(long id) {
		super();
		this.id = id;
	}

	@Override
	public String stringValue() {
		return Long.toString(id);
	}

	@Override
	public String getID() {
		return stringValue();
	}

	public long id() {
		return id;
	}

}
