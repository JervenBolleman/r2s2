package swiss.sib.swissprot.r2s2.loading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.r2rml.TableDescriptionAsRdf;
import swiss.sib.swissprot.r2s2.sql.Columns;
import swiss.sib.swissprot.r2s2.sql.SqlDatatype;
import swiss.sib.swissprot.r2s2.sql.Table;
import swiss.sib.swissprot.r2s2.sql.VirtualSingleValueColumn;

public class TableDescriptionTest {
	private static final String NS = "http://example.org/";
	private static final ValueFactory vf = SimpleValueFactory.getInstance();
	private static final IRI zeroIri = vf.createIRI(NS, "1");
	private static final IRI oneIri = vf.createIRI(NS, "2");

	@Test
	void twoTablesSOG() {
		final Columns zeroSubjectColumns = Columns.from(Kind.IRI, null, null, "subject_", Map.of(), zeroIri);
		final Columns zeroObjectColumns = Columns.from(Kind.IRI, null, null, "object_", Map.of(), zeroIri);
		final Columns oneSubjectColumns = Columns.from(Kind.IRI, null, null, "subject_", Map.of(), oneIri);
		final Columns oneObjectColumns = Columns.from(Kind.IRI, null, null, "object_", Map.of(), oneIri);
		var zero = new Table(zeroIri, zeroSubjectColumns, Kind.IRI, zeroObjectColumns, Kind.IRI, null, null);
		var one = new Table(oneIri, oneSubjectColumns, Kind.IRI, oneObjectColumns, Kind.IRI, null, null);
		final Model model = TableDescriptionAsRdf.model(List.of(zero, one));
		assertNotNull(model);
		assertFalse(model.isEmpty());
		List<Table> tables = TableDescriptionAsRdf.tables(model);
		assertNotNull(tables);
		assertFalse(tables.isEmpty());
		assertTableEquals(zero, tables.get(0));
		assertTableEquals(one, tables.get(1));
	}

	@Test
	void twoTablesVirualSOG() {
		final Columns zeroSubjectColumns = Columns.from(Kind.IRI, null, null, "subject_", Map.of(), zeroIri);
		final Columns zeroObjectColumns = Columns.from(Kind.IRI, null, null, "object_", Map.of(), zeroIri);
		final Columns oneSubjectColumns = Columns.from(Kind.IRI, null, null, "subject_", Map.of(), oneIri);
		final Columns oneObjectColumns = new Columns(
				List.of(new VirtualSingleValueColumn("t", SqlDatatype.TEXT, oneIri.stringValue())));
		var zero = new Table(zeroIri, zeroSubjectColumns, Kind.IRI, zeroObjectColumns, Kind.IRI, null, null);
		var one = new Table(oneIri, oneSubjectColumns, Kind.IRI, oneObjectColumns, Kind.IRI, null, null);
		final Model model = TableDescriptionAsRdf.model(List.of(zero, one));
		assertNotNull(model);
		assertFalse(model.isEmpty());
		List<Table> tables = TableDescriptionAsRdf.tables(model);
		assertNotNull(tables);
		assertFalse(tables.isEmpty());
		assertTableEquals(zero, tables.get(0));
		assertTableEquals(one, tables.get(1));
	}

	private void assertTableEquals(Table expected, Table generated) {
		assertEquals(expected.name(), generated.name());
		assertEquals(expected.subject(), generated.subject());
		assertEquals(expected.subjectKind(), generated.subjectKind());
		assertEquals(expected.objects(), generated.objects());
		assertEquals(expected, generated);
	}
}
