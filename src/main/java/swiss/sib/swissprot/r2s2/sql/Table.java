package swiss.sib.swissprot.r2s2.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.r2s2.loading.Loader.Kind;
import swiss.sib.swissprot.r2s2.loading.TemporaryIriIdMap.TempIriId;

public class Table {
	private final Columns subject;
	private final TempIriId predicate;
	private final Columns object;
	private final Column graphColumn;
	private final Kind subjectKind;
	private final Kind objectKind;

	public Table(TempIriId predicate, Columns subject, Kind subjectKind, Columns object, Kind objectKind,
			Column graphColumn) {
		super();
		this.predicate = predicate;
		this.subject = subject;
		this.subjectKind = subjectKind;
		this.object = object;
		this.objectKind = objectKind;
		this.graphColumn = graphColumn;
	}

	public Columns subject() {
		return subject;
	}

	public Columns object() {
		return object;
	}

	public void create(Connection conn) throws SQLException {
		try (Statement ct = conn.createStatement()) {
			String dml = "CREATE OR REPLACE TABLE " + name() + " (" + subject.definition() + ", " + object.definition()
					+ ", " + graphColumn.definition() + ")";
			LoggerFactory.getLogger(this.getClass()).warn("Running: " + dml);
			ct.execute(dml);
		}

	}

	public String name() {
		return "p_" + predicate.id() + "_" + subjectKind.label() + "_" + objectKind.label();
	}

	public Column graph() {
		return graphColumn;
	}
	
	public void optimizeForR2RML() {
		
	}
}
