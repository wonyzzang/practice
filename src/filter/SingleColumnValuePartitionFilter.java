package filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.ValuePartition;

public class SingleColumnValuePartitionFilter extends SingleColumnValueFilter {

	
	private boolean foundColumn = false;
	private boolean matchedColumn = false;
	private ValuePartition valuePartition = null;

	public SingleColumnValuePartitionFilter(final byte[] family, final byte[] qualifier, final CompareOp compareOp,
			final byte[] value, ValuePartition vp) {
		super(family, qualifier, compareOp, value);
		this.valuePartition = vp;
	}
	
	public SingleColumnValuePartitionFilter(final byte[] family, final byte[] qualifier,
		      final CompareOp compareOp, final ByteArrayComparable comparator, ValuePartition vp) {
		    super(family, qualifier, compareOp, comparator);
		    this.valuePartition = vp;
		  }

	public ValuePartition getValuePartition() {
		return valuePartition;
	}

	@Override
	public boolean filterRow() {
		return this.foundColumn ? !this.matchedColumn : this.getFilterIfMissing();
	}

	public void reset() {
		foundColumn = false;
		matchedColumn = false;
	}
	
	@Override
	public ReturnCode filterKeyValue(Cell c) {
		if(this.matchedColumn){
			return ReturnCode.INCLUDE;
		}else if (this.getLatestVersionOnly() && this.foundColumn) {
			return ReturnCode.NEXT_ROW;
		}
		
		if(!(Bytes.equals(c.getFamilyArray(), this.columnFamily)&&Bytes.equals(c.getQualifierArray(), this.columnQualifier))){
			return ReturnCode.INCLUDE;
		}
		
		foundColumn = true;
		byte[] value = valuePartition.getPartOfValue(c.getValueArray());
		if (filterColumnValue(value, 0, value.length)) {
			return this.getLatestVersionOnly() ? ReturnCode.NEXT_ROW : ReturnCode.INCLUDE;
		}
		this.matchedColumn = true;
		return ReturnCode.INCLUDE;

	}

	private boolean filterColumnValue(final byte[] data, final int offset, final int length) {
		int compareResult = this.getComparator().compareTo(data, offset, length);
		switch (this.getOperator()) {
		case LESS:
			return compareResult <= 0;
		case LESS_OR_EQUAL:
			return compareResult < 0;
		case EQUAL:
			return compareResult != 0;
		case NOT_EQUAL:
			return compareResult == 0;
		case GREATER_OR_EQUAL:
			return compareResult > 0;
		case GREATER:
			return compareResult >= 0;
		default:
			throw new RuntimeException("Unknown Compare op " + this.getOperator().name());
		}
	}
}
