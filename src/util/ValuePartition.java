package util;

import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public interface ValuePartition extends WritableComparable<ValuePartition>, Serializable {
	public enum PartitionType {
		SEPARATOR, SPATIAL, NONE
	}

	public PartitionType getPartitionType();

	public byte[] getPartOfValue(byte[] value);

}
