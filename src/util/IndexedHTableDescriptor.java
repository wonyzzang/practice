package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

/**
 * IndexedHTabledDescriptor is extension of HTableDescriptor. This contains
 * indices to specify index name and column details. There can be one or more
 * indices on one table. For each of the index on the table and
 * IndexSpecification is to be created and added to the indices.
 */
public class IndexedHTableDescriptor extends HTableDescriptor {

	private List<IndexSpecification> indices = new ArrayList<IndexSpecification>(1);

	public IndexedHTableDescriptor() {

	}

	public IndexedHTableDescriptor(String tableName) {
		super(TableName.valueOf(tableName));
	}

	public IndexedHTableDescriptor(byte[] tableName) {
		super(TableName.valueOf(tableName));
	}

	/**
	 * @param IndexSpecification
	 *            to be added to indices
	 * @throws IllegalArgumentException
	 *             if duplicate indexes for same table
	 */
	public void addIndex(IndexSpecification iSpec) throws IllegalArgumentException {
		String indexName = iSpec.getName();
		if (null == indexName) {
			throw new IllegalArgumentException();
		}

		if (true == StringUtils.isBlank(indexName)) {
			throw new IllegalArgumentException();
		}

		if (indexName.length() > Constants.DEF_MAX_INDEX_NAME_LENGTH) {
			throw new IllegalArgumentException();
		}

		for (IndexSpecification is : indices) {
			if (is.getName().equals(indexName)) {
				throw new IllegalArgumentException();
			}
		}
		indices.add(iSpec);
	}

	/**
	 * @return IndexSpecification list
	 */
	public List<IndexSpecification> getIndices() {
		return (new ArrayList<IndexSpecification>(this.indices));
	}

	/**
	 * @param DataOutput
	 *            stream
	 */
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(this.indices.size());
		for (IndexSpecification index : indices) {
			index.write(out);
		}
	}

	/**
	 * @param DataInput
	 *            stream
	 * @throws IOException
	 */
	public void readFields(DataInput in) throws IOException {
		try {
			super.readFields(in);
			int indicesSize = in.readInt();
			indices.clear();
			for (int i = 0; i < indicesSize; i++) {
				IndexSpecification is = new IndexSpecification();
				is.readFields(in);
				this.indices.add(is);
			}
		} catch (EOFException e) {
			throw e;
		}

	}

}
