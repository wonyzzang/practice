package idxcop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class IndexRegionScannerForOR implements IndexRegionScanner {

	private static final Log LOG = LogFactory.getLog(IndexRegionScannerForOR.class);

	private List<IndexRegionScanner> scanners = null;

	// private Pair<IndexRegionScanner, KeyValue> lastReturnedValue = null;

	private byte[] lastReturnedValue = null;

	// Cache to store the values retrieved by range scans
	private Map<String, Boolean> rowCache = new HashMap<String, Boolean>();

	private Map<byte[], Pair<IndexRegionScanner, KeyValue>> valueMap = new TreeMap<byte[], Pair<IndexRegionScanner, KeyValue>>(
			Bytes.BYTES_COMPARATOR);

	private boolean hasRangeScanners = false;

	private boolean isRootScanner = false;

	private int scannerIndex = -1;

	private boolean firstScan = true;

	private boolean reseeked = false;

	public IndexRegionScannerForOR(List<IndexRegionScanner> scanners) {
		this.scanners = scanners;
	}

	@Override
	public void advance() {
		if (this.lastReturnedValue != null) {
			// this.lastReturnedValue.getFirst().advance();
		}
		this.lastReturnedValue = null;
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return null;
	}

	@Override
	public boolean isFilterDone() {
		return false;
	}

	@Override
	public void setRangeFlag(boolean range) {
		hasRangeScanners = range;
	}

	public void setRootFlag(boolean isRootScanner) {
		this.isRootScanner = isRootScanner;
	}

	@Override
	public boolean isRange() {
		return this.hasRangeScanners;
	}

	@Override
	public void setScannerIndex(int index) {
		scannerIndex = index;
	}

	@Override
	public int getScannerIndex() {
		return scannerIndex;
	}

	@Override
	public boolean hasChildScanners() {
		return !scanners.isEmpty();
	}

	@Override
	public synchronized boolean reseek(byte[] row) throws IOException {
		boolean success = false;
		if (!valueMap.isEmpty()) {
			Iterator<Entry<byte[], Pair<IndexRegionScanner, KeyValue>>> itr = valueMap.entrySet().iterator();
			while (itr.hasNext()) {
				Entry<byte[], Pair<IndexRegionScanner, KeyValue>> entry = itr.next();
				IndexRegionScanner scn = entry.getValue().getFirst();
				if (Bytes.compareTo(entry.getKey(), row) < 0) {
					// If the reseek does not retrieve any row then it means we
					// have reached the end of the
					// scan. So this scanner can be safely removed.
					if (!scn.reseek(row)) {
						removeScanner(scn.getScannerIndex());
					}
					itr.remove();
				} else {
					break;
				}
			}
		}
		reseeked = true;
		this.lastReturnedValue = null;
		return success;
	}

	private void removeScanner(int scnIndex) {
		Iterator<IndexRegionScanner> itr = this.scanners.iterator();
		while (itr.hasNext()) {
			if (itr.next().getScannerIndex() == scnIndex) {
				itr.remove();
				break;
			}
		}
	}

	@Override
	public synchronized void close() throws IOException {
		for (IndexRegionScanner scn : this.scanners) {
			scn.close();
		}
		this.valueMap.clear();
		this.scanners.clear();
		this.lastReturnedValue = null;
	}

	@Override
	public synchronized boolean next(List<KeyValue> results) throws IOException {
		OUTER: while (results.size() < 1) {
			List<KeyValue> intermediateResult = new ArrayList<KeyValue>();
			if (hasRangeScanners || firstScan || reseeked) {
				Iterator<IndexRegionScanner> scnItr = this.scanners.iterator();
				INNER: while (scnItr.hasNext()) {
					IndexRegionScanner scn = scnItr.next();
					if (reseeked) {
						boolean exists = checkForScanner(scn.getScannerIndex());
						if (exists)
							continue;
					}
					boolean hasMore = scn.next(intermediateResult);
					if (!hasRangeScanners) {
						if (intermediateResult != null && intermediateResult.size() > 0) {
							byte[] rowKeyFromKV = IndexUtils.getRowKeyFromKV(intermediateResult.get(0));
							while (valueMap.containsKey(rowKeyFromKV)) {
								intermediateResult.clear();
								hasMore = scn.next(intermediateResult);
								if (!intermediateResult.isEmpty()) {
									rowKeyFromKV = IndexUtils.getRowKeyFromKV(intermediateResult.get(0));
								} else {
									break;
								}
							}
							if (!hasMore && intermediateResult.isEmpty()) {
								// Allow other scanners to scan. Nothing to do.
							} else {
								valueMap.put(rowKeyFromKV,
										new Pair<IndexRegionScanner, KeyValue>(scn, intermediateResult.get(0)));
								intermediateResult.clear();
							}
						}
					}
					if (!hasMore) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Removing scanner " + scn + " from the list.");
						}
						scn.close();
						scnItr.remove();
					}
					if (hasRangeScanners) {
						if (!intermediateResult.isEmpty()) {
							String rowKey = Bytes.toString(IndexUtils.getRowKeyFromKV(intermediateResult.get(0)));
							if (isRootScanner && !rowCache.containsKey(rowKey)) {
								rowCache.put(rowKey, false);
								results.addAll(intermediateResult);
								return !this.scanners.isEmpty();
							} else if (isRootScanner) {
								// dont add to results because already exists
								// and scan for other entry.
								intermediateResult.clear();
								continue OUTER;
							} else {
								results.addAll(intermediateResult);
								return !this.scanners.isEmpty();
							}
						}
					}
				}
				if (firstScan)
					firstScan = false;
				if (reseeked)
					reseeked = false;
			} else {
				// Scan on previous scanner which returned minimum values.
				Entry<byte[], Pair<IndexRegionScanner, KeyValue>> minEntry = null;
				if (!valueMap.isEmpty()) {
					minEntry = valueMap.entrySet().iterator().next();
					IndexRegionScanner scn = minEntry.getValue().getFirst();
					if (Bytes.compareTo(lastReturnedValue, minEntry.getKey()) == 0) {
						valueMap.remove(minEntry.getKey());
						boolean hasMore = scn.next(intermediateResult);
						byte[] rowKeyFromKV = null;
						if (!intermediateResult.isEmpty()) {
							rowKeyFromKV = IndexUtils.getRowKeyFromKV(intermediateResult.get(0));
							while (valueMap.containsKey(rowKeyFromKV)) {
								intermediateResult.clear();
								if (hasMore) {
									hasMore = minEntry.getValue().getFirst().next(intermediateResult);
								}
								if (intermediateResult.isEmpty()) {
									rowKeyFromKV = null;
									scn.close();
									removeScanner(scn.getScannerIndex());
									break;
								}
								rowKeyFromKV = IndexUtils.getRowKeyFromKV(intermediateResult.get(0));
							}
						}
						if (rowKeyFromKV != null) {
							valueMap.put(rowKeyFromKV,
									new Pair<IndexRegionScanner, KeyValue>(scn, intermediateResult.get(0)));
							intermediateResult.clear();
						}
					}
				} else {
					return false;
				}
			}
			if (!valueMap.isEmpty()) {
				Entry<byte[], Pair<IndexRegionScanner, KeyValue>> minEntry = valueMap.entrySet().iterator().next();
				lastReturnedValue = minEntry.getKey();
				results.add(minEntry.getValue().getSecond());
				return true;
			} else {
				return false;
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(results.size() + " seek points obtained. Values: "
					+ (!results.isEmpty() ? Bytes.toString(results.get(0).getRow()) : 0));
		}
		return !results.isEmpty();
	}

	private boolean checkForScanner(int scnIndex) {
		Collection<Pair<IndexRegionScanner, KeyValue>> scanners = valueMap.values();
		for (Pair<IndexRegionScanner, KeyValue> scn : scanners) {
			if (scnIndex == scn.getFirst().getScannerIndex()) {
				return true;
			}
		}
		return false;
	}

	/*
	 * private void getTheSmallerValue(KeyValue keyValue, IndexRegionScanner scn
	 * ) { byte[] currentRow = getRowKeyFromKV(keyValue);
	 * if(this.lastReturnedValue == null){ this.lastReturnedValue.setFirst(new
	 * Pair<byte[], Integer>(currentRow,scn.getScannerIndex()));
	 * this.lastReturnedValue.setSecond(keyValue); } else { byte[] lastRow =
	 * getRowKeyFromKV(this.lastReturnedValue.getSecond());
	 * if(Bytes.compareTo(currentRow, lastRow) < 0){
	 * this.lastReturnedValue.setFirst(new Pair<byte[],
	 * Integer>(currentRow,scn.getScannerIndex()));
	 * this.lastReturnedValue.setSecond(keyValue); } // TODO: In case of equal
	 * need to check what to do? if(Bytes.compareTo(currentRow, lastRow) == 0){
	 * scn.advance(); } } }
	 */

	@Override
	public boolean next(List<KeyValue> result, int limit) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getMvccReadPoint() {
		// TODO Implement RegionScanner.getMvccReadPoint
		return 0;
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
		// TODO Implement RegionScanner.nextRaw
		return false;
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
		// TODO Implement RegionScanner.nextRaw
		return false;
	}

	@Override
	public boolean next(List<KeyValue> results, String metric) throws IOException {
		// TODO Implement InternalScanner.next
		return false;
	}

	@Override
	public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
		// TODO Implement InternalScanner.next
		return false;
	}

}
