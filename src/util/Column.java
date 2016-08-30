package util;

import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;

public class Column implements Serializable{
	private static final long serialVersionUID = -1958705310924323448L;

	  private byte[] cf;
	  private byte[] qualifier;
	  private ValuePartition valuePartition = null;

	  public Column() {
	  }

	  public Column(byte[] cf, byte[] qualifier) {
	    this.cf = cf;
	    this.qualifier = qualifier;
	  }

	  public Column(byte[] cf, byte[] qualifier, ValuePartition vp) {
	    this.cf = cf;
	    this.qualifier = qualifier;
	    this.valuePartition = vp;
	  }

	  public void setFamily(byte[] cf) {
	    this.cf = cf;
	  }

	  public void setQualifier(byte[] qualifier) {
	    this.qualifier = qualifier;
	  }

	  public byte[] getFamily() {
	    return cf;
	  }

	  public byte[] getQualifier() {
	    return qualifier;
	  }

	  public ValuePartition getValuePartition() {
	    return this.valuePartition;
	  }

	  public void setValuePartition(ValuePartition vp) {
	    this.valuePartition = vp;
	  }

	  public boolean equals(Object obj) {
	    if (!(obj instanceof Column)) return false;
	    Column that = (Column) obj;
	    if (!(Bytes.equals(this.cf, that.cf))) return false;
	    if (!(Bytes.equals(this.qualifier, that.qualifier))) return false;
	    if (valuePartition == null && that.valuePartition == null) {
	      return true;
	    } else if (valuePartition != null && that.valuePartition != null) {
	      return valuePartition.equals(that.valuePartition);
	    } else {
	      return false;
	    }
	  }

	  public int hashCode() {
	    int result = Bytes.hashCode(this.cf);
	    result ^= Bytes.hashCode(this.qualifier);
	    if (valuePartition != null) result ^= valuePartition.hashCode();
	    return result;
	  }

	  public String toString() {
	    return Bytes.toString(this.cf) + " : " + Bytes.toString(this.qualifier);
	  }
}
