package swiss.sib.swissprot.r2s2.sql;

public enum Datatypes {
	BOOLEAN("boolean"), NUMERIC("numeric"), TEXT("text"), DATE("date"), TIMESTAMP("timestamp"), INTERVAL("interval"),
	BLOB("blob"), LIST("list"), STRUCT("struct"), MAP("map"), UNION("union"), BIGINT("bigint");

	private final String sql;

	Datatypes(String sql) {
		this.sql = sql;
	}

	public String label() {
		return sql;
	}
}
