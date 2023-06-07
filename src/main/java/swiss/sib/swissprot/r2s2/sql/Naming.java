package swiss.sib.swissprot.r2s2.sql;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;

import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap.TempIriId;

public class Naming {
	private Naming() {

	}

	public static String iriToSqlNamePart(Map<String, String> namespaces, IRI iri) {
		CoreDatatype cdt = CoreDatatype.from(iri);
		if (cdt != null) {
			if (cdt.isXSDDatatype()) {
				return "xsd_" + iri.getLocalName();
			} else if (cdt.isRDFDatatype()) {
				return "rdf_" + iri.getLocalName();
			} else if (cdt.isGEODatatype()) {
				return "geo_" + iri.getLocalName();
			}
		}
		String iriS = iri.stringValue();
		for (Map.Entry<String, String> en : namespaces.entrySet()) {
			if (iriS.startsWith(en.getValue()) && !en.getKey().isEmpty()) {
				final String preName = en.getKey() + "_" + iriS.substring(en.getValue().length());
				return preName;
			}
		}
		byte[] encode;
		try {
			encode = Base64.getEncoder().encode(iri.stringValue().getBytes(StandardCharsets.UTF_8));
			//can't have the last = 
			return new String(encode, 0, encode.length - 1, StandardCharsets.UTF_8);
		} catch (Exception e) {
			if (iri instanceof TempIriId) {
				return "__" + ((TempIriId) iri).id();
			}
			throw new RuntimeException(e);
		}
	}
}
