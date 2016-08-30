package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import util.ValuePartition.PartitionType;

public class ColumnQualifier implements WritableComparable<ColumnQualifier>{
	private byte[] cfBytes;

	  private byte[] qualifierBytes;

	  private int maxValueLength;

	  private ValuePartition valuePartition = null;

	  private ValueType type;

	  public ColumnQualifier() {
	    // Dummy constructor which is needed for the readFields
	  }

	  public ColumnQualifier(String cf, String qualifier) {
	    this(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
	  }

	  public ColumnQualifier(byte[] cf, byte[] qualifier) {
	    this(cf, qualifier, ValueType.String, 0, null);
	  }

	  public ColumnQualifier(String cf, String qualifier, ValueType type, int maxValueLength) {
	    this(Bytes.toBytes(cf), Bytes.toBytes(qualifier), type, maxValueLength, null);
	  }

	  public ColumnQualifier(String cf, String qualifier, ValueType type, int maxValueLength,
	      ValuePartition vp) {
	    this(Bytes.toBytes(cf), Bytes.toBytes(qualifier), type, maxValueLength, vp);
	  }

	  public ColumnQualifier(byte[] cf, byte[] qualifier, ValueType type, int maxValueLength,
	      ValuePartition vp) {
	    this.cfBytes = cf;
	    this.qualifierBytes = qualifier;
	    this.type = type;
	    this.maxValueLength = maxValueLength;
	    this.valuePartition = vp;
	  }

	  /**
	   * @return Column Family as string
	   */
	  public String getColumnFamilyString() {
	    return Bytes.toString(this.cfBytes);
	  }

	  /**
	   * @return Column qualifier as string
	   */
	  public String getQualifierString() {
	    return Bytes.toString(this.qualifierBytes);
	  }

	  /**
	   * @return Column family as byte array
	   */
	  public byte[] getColumnFamily() {
	    return this.cfBytes;
	  }

	  /**
	   * @return Column qualifier as byte array
	   */
	  public byte[] getQualifier() {
	    return this.qualifierBytes;
	  }

	  public ValuePartition getValuePartition() {
	    return valuePartition;
	  }

	  /**
	   * @param DataInput Stream
	   * @throws IOException
	   */
	  public void readFields(DataInput in) throws IOException {
	    this.cfBytes = Bytes.readByteArray(in);
	    this.qualifierBytes = Bytes.readByteArray(in);
	    this.type = ValueType.valueOf(Bytes.toString(Bytes.readByteArray(in)));
	    this.maxValueLength = in.readInt();
	    PartitionType p = PartitionType.valueOf(in.readUTF());
	    if (p.equals(PartitionType.SEPARATOR)) {
	      valuePartition = new SeparatorPartition();
	    } else if (p.equals(PartitionType.SPATIAL)) {
	      valuePartition = new SpatialPartition();
	    }
	    if (valuePartition != null) {
	      valuePartition.readFields(in);
	    }
	  }

	  /**
	   * @param DataOutput stream
	   * @throws IOException
	   */
	  public void write(DataOutput out) throws IOException {
	    Bytes.writeByteArray(out, this.cfBytes);
	    Bytes.writeByteArray(out, this.qualifierBytes);
	    Bytes.writeByteArray(out, Bytes.toBytes(this.type.name()));
	    out.writeInt(maxValueLength);
	    if (valuePartition == null) {
	      out.writeUTF(PartitionType.NONE.name());
	    } else {
	      out.writeUTF(valuePartition.getPartitionType().name());
	      valuePartition.write(out);
	    }
	  }

	  /**
	   * @param ColumnQualifier with whom to compare
	   * @return return true if both objects are equal otherwise false
	   */
	  @Override
	  public boolean equals(Object cq) {
	    if (this == cq) {
	      return true;
	    }
	    if (false == (cq instanceof ColumnQualifier)) {
	      return false;
	    }
	    return this.compareTo((ColumnQualifier) cq) == 0;
	  }

	  /**
	   * return hashcode of object
	   */
	  public int hashCode() {
	    int result = Bytes.hashCode(this.cfBytes);
	    result ^= Bytes.hashCode(this.qualifierBytes);
	    result ^= this.maxValueLength;
	    if (valuePartition != null) result ^= valuePartition.hashCode();
	    return result;
	  }

	  /**
	   * @param IndexSpecification
	   * @return int
	   */
	  @Override
	  public int compareTo(ColumnQualifier cq) {
	    int diff = 0;
	    diff = Bytes.compareTo(this.cfBytes, cq.cfBytes);
	    if (0 == diff) {
	      diff = Bytes.compareTo(this.qualifierBytes, cq.qualifierBytes);
	      if (0 == diff) {
	        if (valuePartition != null && cq.valuePartition != null) {
	          return valuePartition.compareTo(cq.valuePartition);
	        } else if (valuePartition == null && cq.valuePartition == null) {
	          return 0;
	        } else {
	          return 1;
	        }
	      }
	    }
	    return diff;
	  }

	  public int getMaxValueLength() {
	    return this.maxValueLength;
	  }

	  public ValueType getType() {
	    return this.type;
	  }

	  public enum ValueType {
	    String, Int, Float, Long, Double, Short, Byte, Char
	  };

	  // TODO - Include valuePartition also into this
	  public String toString() {
	    return "CF : " + getColumnFamilyString() + ",Qualifier : " + getQualifierString();
	  }
}
