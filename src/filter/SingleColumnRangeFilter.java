package filter;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import util.ValuePartition;

public class SingleColumnRangeFilter extends FilterBase {

	private byte[] family;
	private byte[] qualifier;

	private CompareOp lowerCompareOp;
	private CompareOp upperCompareop;

	private byte[] upperBoundValue;
	private byte[] lowerBoundValue;

	private ValuePartition valuePartition = null;

	public SingleColumnRangeFilter(byte[] cf, byte[] qualifier, byte[] boundValue1, CompareOp boundOp1,
			byte[] boundValue2, CompareOp boundOp2) {

		this.family = cf;
		this.qualifier = qualifier;

		if (boundOp1.equals(CompareOp.LESS) || boundOp1.equals(CompareOp.LESS_OR_EQUAL)) {
			this.upperCompareop = boundOp1;
			this.upperBoundValue = boundValue1;
			this.lowerCompareOp = boundOp2;
			this.lowerBoundValue = boundValue2;
		} else {
			this.upperCompareop = boundOp2;
			this.upperBoundValue = boundValue2;
			this.lowerCompareOp = boundOp1;
			this.lowerBoundValue = boundValue1;
		}
	}

	public SingleColumnRangeFilter(byte[] cf, byte[] qualifier, ValuePartition vp, byte[] boundValue1,
			CompareOp boundOp1, byte[] boundValue2, CompareOp boundOp2) {
		this(cf, qualifier, boundValue1, boundOp1, boundValue2, boundOp2);
		this.valuePartition = vp;
	}

	public void setUpperBoundValue(byte[] upperBoundValue, CompareOp upperOp) {
		this.upperBoundValue = upperBoundValue;
		this.upperCompareop = upperOp;
	}

	public void setLowerBoundValue(byte[] lowerBoundValue, CompareOp lowerOp) {
		this.lowerBoundValue = lowerBoundValue;
		this.lowerCompareOp = lowerOp;
	}

	public byte[] getFamily() {
		return this.family;
	}

	public byte[] getQualifier() {
		return this.qualifier;
	}

	public CompareOp getUpperBoundOp() {
		return this.upperCompareop;
	}

	public CompareOp getLowerBoundOp() {
		return this.lowerCompareOp;
	}

	public byte[] getLowerBoundValue() {
		return this.lowerBoundValue;
	}

	public byte[] getUpperBoundValue() {
		return this.upperBoundValue;
	}

	public ValuePartition getValuePartition() {
		return valuePartition;
	}

	public void setValuePartition(ValuePartition valuePartition) {
		this.valuePartition = valuePartition;
	}

	public String toString() {
		return String.format("%s (%s, %s, %s, %s, %s, %s)", this.getClass().getSimpleName(),
				Bytes.toStringBinary(this.family), Bytes.toStringBinary(this.qualifier),
				this.lowerCompareOp == null ? "" : this.lowerCompareOp.name(),
				this.lowerBoundValue == null ? "" : Bytes.toStringBinary(this.lowerBoundValue),
				this.upperCompareop == null ? "" : this.upperCompareop.name(),
				this.upperBoundValue == null ? "" : Bytes.toStringBinary(this.upperBoundValue));
	}

	@Override
	public ReturnCode filterKeyValue(Cell arg0) throws IOException {
		return null;
	}
}
