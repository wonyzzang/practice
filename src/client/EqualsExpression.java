package client;

import java.io.Serializable;

import util.Column;

public class EqualsExpression implements Serializable {
	private static final long serialVersionUID = -7130697408286943018L;

	private Column column;
	private byte[] value;

	public EqualsExpression(Column column, byte[] value) {
		this.column = column;
		this.value = value;
	}

	public Column getColumn() {
		return this.column;
	}

	public byte[] getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "EqualsExpression : Column[" + this.column + ']';
	}

}
