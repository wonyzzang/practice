package manager;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * IndexManager manages the index details of each table.
 */
public class IndexManager {

	// manager is Singleton object
	private static IndexManager manager = new IndexManager();

	private Map<String, List<IndexSpecification>> tableVsIndices = new ConcurrentHashMap<String, List<IndexSpecification>>();

	private ConcurrentHashMap<String, AtomicInteger> tableVsNumberOfRegions = new ConcurrentHashMap<String, AtomicInteger>();

	// TODO one DS is enough
	private Map<String, Map<byte[], IndexSpecification>> tableIndexMap = new ConcurrentHashMap<String, Map<byte[], IndexSpecification>>();

	private IndexManager() {

	}

	/**
	 * @return IndexManager instance
	 */
	public static IndexManager getInstance() {
		return manager;
	}

	/**
	 * @param table
	 *            name on which index applying
	 * @param IndexSpecification
	 *            list of table
	 */
	public void addIndexForTable(String tableName, List<IndexSpecification> indexList) {
		this.tableVsIndices.put(tableName, indexList);
		// TODO the inner map needs to be thread safe when we support dynamic
		// index add/remove
		Map<byte[], IndexSpecification> indexMap = new TreeMap<byte[], IndexSpecification>(Bytes.BYTES_COMPARATOR);
		for (IndexSpecification index : indexList) {
			ByteArrayBuilder keyBuilder = ByteArrayBuilder.allocate(IndexUtils.getMaxIndexNameLength());
			keyBuilder.put(Bytes.toBytes(index.getName()));
			indexMap.put(keyBuilder.array(), index);
		}
		this.tableIndexMap.put(tableName, indexMap);
	}

	/**
	 * @param table
	 *            name on which index applying
	 * @return IndexSpecification list for the table or return null if no index
	 *         for the table
	 */
	public List<IndexSpecification> getIndicesForTable(String tableName) {
		return this.tableVsIndices.get(tableName);
	}

	/**
	 * @param tableName
	 *            on which index applying
	 */
	public void removeIndices(String tableName) {
		this.tableVsIndices.remove(tableName);
		this.tableIndexMap.remove(tableName);
	}

	/**
	 * @param tableName
	 * @param indexName
	 * @return
	 */
	public IndexSpecification getIndex(String tableName, byte[] indexName) {
		Map<byte[], IndexSpecification> indices = this.tableIndexMap.get(tableName);
		if (indices != null) {
			return indices.get(indexName);
		}
		return null;
	}

	public void incrementRegionCount(String tableName) {
		AtomicInteger count = this.tableVsNumberOfRegions.get(tableName);
		// Here synchronization is needed for the first time count operation to
		// be
		// initialized
		if (null == count) {
			synchronized (tableVsNumberOfRegions) {
				count = this.tableVsNumberOfRegions.get(tableName);
				if (null == count) {
					count = new AtomicInteger(0);
					this.tableVsNumberOfRegions.put(tableName, count);
				}
			}
		}
		count.incrementAndGet();

	}

	public void decrementRegionCount(String tableName, boolean removeIndices) {
		// Need not be synchronized here because anyway the decrement operator
		// will work atomically. Ultimately atleast one thread will see the
		// count
		// to be 0 which should be sufficient to remove the indices
		AtomicInteger count = this.tableVsNumberOfRegions.get(tableName);
		if (null != count) {
			int next = count.decrementAndGet();
			if (next == 0) {
				this.tableVsNumberOfRegions.remove(tableName);
				if (removeIndices) {
					this.removeIndices(tableName);
				}
			}
		}
	}

	// API needed for test cases.
	public int getTableRegionCount(String tableName) {
		AtomicInteger count = this.tableVsNumberOfRegions.get(tableName);
		if (count != null) {
			return count.get();
		}
		return 0;
	}
}
