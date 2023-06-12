package swiss.sib.swissprot.r2s2.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;

public class Table {
	private static final Logger log = LoggerFactory.getLogger(Table.class);
	private final Columns subject;
	private final List<PredicateMap> objects = new ArrayList<>();

	private final Kind subjectKind;
	private final String name;
	private static final AtomicInteger ID_GEN = new AtomicInteger();

	public Table(IRI predicate, Columns subject, Kind subjectKind, Columns object, Kind objectKind, String lang,
			IRI datatype) {
		super();
		this.subject = subject;
		this.subjectKind = subjectKind;
		this.objects.add(new PredicateMap(predicate, object, objectKind, lang, datatype));

		this.name = generateName("_pred_" + ID_GEN.incrementAndGet(), subjectKind, objectKind, lang, datatype);
	}

	public static String generateName(String name, Kind subjectKind, Kind objectKind, String lang, IRI datatype) {
		name = name + "_" + subjectKind.label() + "_" + objectKind.label();
		name = addLangDatatype(lang, datatype, name);
		return name;
	}

	public static String addLangDatatype(String lang, IRI datatype, String name) {
		if (lang != null) {
			name += "_" + lang.replace('-', '_');
		} else if (datatype != null) {

			CoreDatatype from = CoreDatatype.from(datatype);
			if (from != null && from.isXSDDatatype()) {
				name += "_xsd_" + datatype.getLocalName();
			} else if (from != null && from.isRDFDatatype()) {
				name += "_rdf_" + datatype.getLocalName();
			} else if (from != null && from.isGEODatatype()) {
				name += "_geo_" + datatype.getLocalName();
			} else {
				name += "_dt";
			}
		}
		return name;
	}

	public Table(String name, Columns subject, Kind subjectKind, List<PredicateMap> objects) {
		super();
		this.subject = subject;
		this.subjectKind = subjectKind;
		this.objects.addAll(objects);
		this.name = name;
	}

	public Columns subject() {
		return subject;
	}

	public List<PredicateMap> objects() {
		return objects;
	}

	public void create(Connection conn) throws SQLException {
		final Stream<String> map = objects.stream()
				.map((p) -> p.columns().definition());
		String objectsDefinition = map
				.filter(Predicate.not(String::isBlank))
				.collect(Collectors.joining(","));
		if (objectsDefinition == null || objectsDefinition.isEmpty()) {
			objectsDefinition = "";
		} else {
			objectsDefinition = ", " + objectsDefinition;
		}
		String dml = "CREATE TABLE " + name() + " (" + subject.definition() + objectsDefinition + ")";
		try (Statement ct = conn.createStatement()) {

			log.warn("Running: " + dml);
			ct.execute(dml);
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		}

	}

	public String name() {

		return name;
	}


	@Override
	public int hashCode() {
		return Objects.hash(objects, name, subject, subjectKind);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Table other = (Table) obj;
		return Objects.equals(objects, other.objects) && name == other.name && Objects.equals(name, other.name)
				&& Objects.equals(subject, other.subject) && subjectKind == other.subjectKind;
	}

	public Kind subjectKind() {
		return subjectKind;
	}


}
