package swiss.sib.swissprot.r2s2.sql;

public class VirtualSingleValueColumn extends Column {

	private final String value;

	public VirtualSingleValueColumn(String name, Datatypes datatype, String value) {
		super(name, datatype);
		this.value = value;
	}

	public String value() {
		return value;
	}

	public boolean isVirtual() {
		return true;
	}
}
