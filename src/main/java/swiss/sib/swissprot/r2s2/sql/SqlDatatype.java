package swiss.sib.swissprot.r2s2.sql;

public enum SqlDatatype {
	BOOLEAN("boolean"), NUMERIC("numeric"), TEXT("text"), DATE("date"), TIMESTAMP("timestamp"), INTERVAL("interval"),
	BLOB("blob"), LIST("list"), STRUCT("struct"), MAP("map"), UNION("union"), INTEGER("integer"), BIGINT("bigint"),
	DOUBLE("double"), FLOAT("float"), GRAPH_IRIS("graph_iris"), PROTOCOL("protocol"), HOST("host");

	private final String sql;

	SqlDatatype(String sql) {
		this.sql = sql;
	}

	public String label() {
		return sql;
	}

	public static SqlDatatype fromLabel(String label) {
		for (SqlDatatype dt : values()) {
			if (dt.label().equals(label)) {
				return dt;
			}
		}
		return null;
	}
}
