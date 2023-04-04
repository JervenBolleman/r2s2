package swiss.sib.swissprot.r2s2.sql;

import org.eclipse.rdf4j.model.IRI;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;

public record PredicateMap(IRI predicate, Columns columns, Kind objectKind, String lang, IRI datatype) {

}
