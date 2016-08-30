package idxcop;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterColumnValueDetail {
	protected byte[] cf;
	protected byte[] qualifier;
	protected byte[] value;
	protected CompareOp compareOp;
	protected Column column;
	protected int maxValueLength;
	protected ValueType valueType;

	public FilterColumnValueDetail(byte[] cf, byte[] qualifier, byte[] value, CompareOp compareOp) {
		this.cf = cf;
		this.qualifier = qualifier;
		this.value = value;
		this.compareOp = compareOp;
		this.column = new Column(this.cf, this.qualifier);
	}

	public FilterColumnValueDetail(byte[] cf, byte[] qualifier, byte[] value, ValuePartition valuePartition,
			CompareOp compareOp) {
		this.cf = cf;
		this.qualifier = qualifier;
		this.value = value;
		this.compareOp = compareOp;
		this.column = new Column(this.cf, this.qualifier, valuePartition);
	}

	public FilterColumnValueDetail(Column column, byte[] value, CompareOp compareOp) {
		this.cf = column.getFamily();
		this.qualifier = column.getQualifier();
		this.value = value;
		this.compareOp = compareOp;
		this.column = column;
	}

	public boolean equals(Object obj) {
		if (!(obj instanceof FilterColumnValueDetail))
			return false;
		FilterColumnValueDetail that = (FilterColumnValueDetail) obj;
		if (!this.column.equals(that.column)) {
			return false;
		}
		// Need to check.
		if (this.value != null && that.value != null) {
			if (!(Bytes.equals(this.value, that.value)))
				return false;
		} else if (this.value == null && that.value == null) {
			return true;
		} else {
			return false;
		}
		return true;
	}

	public int hashCode() {
		return this.column.hashCode();
	}

	public String toString() {
		return String.format("%s (%s, %s, %s, %s, %s)", this.getClass().getSimpleName(), Bytes.toStringBinary(this.cf),
				Bytes.toStringBinary(this.qualifier), this.valueType.name(), this.compareOp.name(),
				Bytes.toStringBinary(this.value));
	}

	public Column getColumn() {
		return this.column;
	}

	public byte[] getValue() {
		return this.value;
	}

	protected void setValue(byte[] value) {
		this.value = value;
	}

}
