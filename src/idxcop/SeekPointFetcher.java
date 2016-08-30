package idxcop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

// TODO better name
public class SeekPointFetcher {

	private static final Log LOG = LogFactory.getLog(SeekPointFetcher.class);
	private RegionScanner indexRegionScanner;

	public SeekPointFetcher(RegionScanner indexRegionScanner) {
		this.indexRegionScanner = indexRegionScanner;
	}

	/**
	 * Fetches the next N seek points for the scan.
	 * 
	 * @param seekPoints
	 * @param noOfSeekPoints
	 * @return false when the scan on the index table for having no more rows
	 *         remaining.
	 * @throws IOException
	 */
	public synchronized boolean nextSeekPoints(List<byte[]> seekPoints, int noOfSeekPoints) throws IOException {
		boolean hasMore = true;
		List<KeyValue> indexScanResult = new ArrayList<KeyValue>();
		for (int i = 0; i < noOfSeekPoints; i++) {
			hasMore = indexRegionScanner.next(indexScanResult);
			if (indexScanResult.size() > 0) {
				populateSeekPointsWithTableRowKey(seekPoints, indexScanResult.get(0));
			}
			indexScanResult.clear();
			if (hasMore == false)
				break;
		}
		// TODO log the seekpoints INFO level.
		return hasMore;
	}

	private void populateSeekPointsWithTableRowKey(List<byte[]> seekPoints, KeyValue kv) {
		byte[] row = kv.getRow();
		// Row key of the index table entry = region startkey + index name +
		// column value(s)
		// + actual table rowkey.
		// Every row in the index table will have exactly one KV in that. The
		// value will be
		// 4 bytes. First 2 bytes specify length of the region start key bytes
		// part in the
		// rowkey. Last 2 bytes specify the offset to the actual table rowkey
		// part within the
		// index table rowkey.
		byte[] value = kv.getValue();
		short actualRowKeyOffset = Bytes.toShort(value, 2);
		if (LOG.isTraceEnabled()) {
			LOG.trace("row value for the index table " + Bytes.toString(row));
		}
		byte[] actualTableRowKey = new byte[row.length - actualRowKeyOffset];
		System.arraycopy(row, actualRowKeyOffset, actualTableRowKey, 0, actualTableRowKey.length);
		seekPoints.add(actualTableRowKey);
	}

	public synchronized void close() throws IOException {
		this.indexRegionScanner.close();
	}
}
