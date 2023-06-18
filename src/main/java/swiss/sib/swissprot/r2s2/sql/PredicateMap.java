package swiss.sib.swissprot.r2s2.sql;

import org.eclipse.rdf4j.model.IRI;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;

public record PredicateMap(IRI predicate, GroupOfColumns groupOfColumns, Kind objectKind, String lang, IRI datatype) {
	
	public PredicateMap copy() {
		return new PredicateMap(predicate, groupOfColumns.copy(), objectKind, lang, datatype);
	}

}
