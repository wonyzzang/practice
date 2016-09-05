package idxcop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

// TODO better name?
public class ReInitializableRegionScannerImpl implements ReInitializableRegionScanner {

	private RegionScanner delegator;

	private int batch;

	private byte[] lastSeekedRowKey;

	private byte[] lastSeekAttemptedRowKey;

	private static final Log LOG = LogFactory.getLog(ReInitializableRegionScannerImpl.class);

	// Can this be queue?
	// Criteria for the selection to be additional heap overhead wrt the object
	// used cost in operation
	private Set<byte[]> seekPoints;

	private SeekPointFetcher seekPointFetcher;

	private boolean closed = false;

	public ReInitializableRegionScannerImpl(RegionScanner delegator, Scan scan, SeekPointFetcher seekPointFetcher) {
		this.delegator = delegator;
		this.batch = scan.getBatch();
		this.seekPoints = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		this.seekPointFetcher = seekPointFetcher;
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return this.delegator.getRegionInfo();
	}

	@Override
	public boolean isFilterDone() {
		boolean isDone = false;
		
		try {
			isDone = this.delegator.isFilterDone();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isDone;
	}

	@Override
	public synchronized void close() throws IOException {
		try {
			this.delegator.close();
		} finally {
			this.seekPointFetcher.close();
		}
		closed = true;
	}

	@Override
	public synchronized boolean next(List<Cell> results) throws IOException {
		return next(results, this.batch);
	}

	public boolean isClosed() {
		return closed;
	}

	@Override
	public synchronized boolean next(List<Cell> result, int limit) throws IOException {
		return next(result, limit, null);
	}

	@Override
	public void addSeekPoints(List<byte[]> seekPoints) {
		// well this add will do the sorting and remove duplicates. :)
		for (byte[] seekPoint : seekPoints) {
			this.seekPoints.add(seekPoint);
		}
	}

	@Override
	public boolean seekToNextPoint() throws IOException {
		// At this class level if seek is called it must be forward seek.
		// call reseek() directly with the next seek point
		Iterator<byte[]> spIterator = this.seekPoints.iterator();
		if (spIterator.hasNext()) {
			this.lastSeekAttemptedRowKey = spIterator.next();
			if (null != this.lastSeekedRowKey
					&& Bytes.BYTES_COMPARATOR.compare(this.lastSeekedRowKey, this.lastSeekAttemptedRowKey) > 0) {
				throw new SeekUnderValueException();
			}
			spIterator.remove();
			LOG.trace("Next seek point " + Bytes.toString(this.lastSeekAttemptedRowKey));
			boolean reseekResult = closed ? false : this.reseek(this.lastSeekAttemptedRowKey);
			if (!reseekResult)
				return false;
			this.lastSeekedRowKey = this.lastSeekAttemptedRowKey;
			return true;
		}
		return false;
	}

	@Override
	public synchronized boolean reseek(byte[] row) throws IOException {
		return this.delegator.reseek(row);
	}

	@Override
	public void reInit(RegionScanner rs) throws IOException {
		this.delegator.close();
		this.delegator = rs;
		this.lastSeekedRowKey = null;
		this.lastSeekAttemptedRowKey = null;
		this.closed = false;
	}

	@Override
	public byte[] getLatestSeekpoint() {
		return this.lastSeekAttemptedRowKey;
	}

	@Override
	public long getMvccReadPoint() {
		return this.delegator.getMvccReadPoint();
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
		return this.delegator.nextRaw(result, metric);
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
		return this.delegator.nextRaw(result, limit, metric);
	}

	@Override
	public boolean next(List<KeyValue> results, String metric) throws IOException {
		// TODO Implement InternalScanner.next
		return false;
	}

	@Override
	public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
		// Before every next call seek to the appropriate position.
		if (closed)
			return false;
		while (!seekToNextPoint()) {
			List<byte[]> seekPoints = new ArrayList<byte[]>(1);
			// TODO Do we need to fetch more seekpoints here?
			if (!closed)
				this.seekPointFetcher.nextSeekPoints(seekPoints, 1);
			if (seekPoints.isEmpty()) {
				// nothing further to fetch from the index table.
				return false;
			}
			this.seekPoints.addAll(seekPoints);
		}
		return closed ? false : this.delegator.next(result, limit, metric);
	}
}
