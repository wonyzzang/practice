package filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

// TODO make it a full fledged Filter implementation
/**
 * This is internal class. Not exposed as a Filter implementation
 */
public class SingleColumnRangeFilter extends FilterBase {

  private byte[] family;

  private byte[] qualifier;

  private CompareOp lowerCompareOp;

  private CompareOp upperCompareop;

  private byte[] upperBoundValue;

  private byte[] lowerBoundValue;

  private ValuePartition valuePartition = null;

  /**
   * When the range is having only one bound pass that value and condition as the boundValue1 and
   * boundOp1. Here boundValue2 and boundOp2 can be passed as <code>null<code>
   * @param cf
   * @param qualifier
   * @param boundValue1
   * @param boundOp1
   * @param boundValue2
   * @param boundOp2
   */
  public SingleColumnRangeFilter(byte[] cf, byte[] qualifier, byte[] boundValue1,
      CompareOp boundOp1, byte[] boundValue2, CompareOp boundOp2) {
    this.family = cf;
    this.qualifier = qualifier;
    // CompareOp.LESS or CompareOp.LESS_OR_EQUAL will be the upper bound
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

  public SingleColumnRangeFilter(byte[] cf, byte[] qualifier, ValuePartition vp,
      byte[] boundValue1, CompareOp boundOp1, byte[] boundValue2, CompareOp boundOp2) {
    this(cf, qualifier, boundValue1, boundOp1, boundValue2, boundOp2);
    this.valuePartition = vp;
  }

  @Override
  public void readFields(DataInput datainput) throws IOException {

  }

  @Override
  public void write(DataOutput dataoutput) throws IOException {

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
    return String.format("%s (%s, %s, %s, %s, %s, %s)", this.getClass().getSimpleName(), Bytes
        .toStringBinary(this.family), Bytes.toStringBinary(this.qualifier),
      this.lowerCompareOp == null ? "" : this.lowerCompareOp.name(),
      this.lowerBoundValue == null ? "" : Bytes.toStringBinary(this.lowerBoundValue),
      this.upperCompareop == null ? "" : this.upperCompareop.name(),
      this.upperBoundValue == null ? "" : Bytes.toStringBinary(this.upperBoundValue));
  }
}
