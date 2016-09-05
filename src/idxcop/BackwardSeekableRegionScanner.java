package idxcop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

public class BackwardSeekableRegionScanner implements SeekAndReadRegionScanner {

	private ReInitializableRegionScanner delegator;

	private Scan scan;

	private HRegion hRegion;

	private byte[] startRow;

	private boolean closed = false;

	public BackwardSeekableRegionScanner(ReInitializableRegionScanner delegator, Scan scan, HRegion hRegion,
			byte[] startRow) {
		this.delegator = delegator;
		this.scan = scan;
		this.hRegion = hRegion;
		this.startRow = startRow;
	}

	Scan getScan() {
		return scan;
	}

	byte[] getStartRow() {
		return startRow;
	}

	// For testing.
	RegionScanner getDelegator() {
		return delegator;
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
			e.printStackTrace();
		}
		return isDone;
	}

	@Override
	public synchronized void close() throws IOException {
		this.delegator.close();
		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public synchronized boolean next(List<Cell> results) throws IOException {
		return next(results, this.scan.getBatch());
	}

	public boolean next(List<Cell> result, int limit) throws IOException {
		return false;
	}

	@Override
	public synchronized boolean reseek(byte[] row) throws IOException {
		return this.delegator.reseek(row);
	}

	@Override
	public void addSeekPoints(List<byte[]> seekPoints) {
		this.delegator.addSeekPoints(seekPoints);
	}

	@Override
	public boolean seekToNextPoint() throws IOException {
		return this.delegator.seekToNextPoint();
	}

	@Override
	public byte[] getLatestSeekpoint() {
		return this.delegator.getLatestSeekpoint();
	}

	public boolean next(List<Cell> results, String metric) throws IOException {
		return next(results, this.scan.getBatch(), metric);
	}

	public boolean next(List<Cell> result, int limit, String metric) throws IOException {
		boolean hasNext = false;
		try {
			if (this.delegator.isClosed())
				return false;
			hasNext = this.delegator.next(result, limit, metric);
		} catch (SeekUnderValueException e) {
			Scan newScan = new Scan(this.scan);
			// Start from the point where we got stopped because of seek
			// backward
			newScan.setStartRow(getLatestSeekpoint());
			this.delegator.reInit(this.hRegion.getScanner(newScan));
			hasNext = next(result, limit, metric);
		}
		return hasNext;
	}

	@Override
	public long getMvccReadPoint() {
		return this.delegator.getMvccReadPoint();
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
		return next(result, metric);
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
		return next(result, limit, metric);
	}

	@Override
	public int getBatch() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getMaxResultSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean nextRaw(List<Cell> arg0) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nextRaw(List<Cell> arg0, ScannerContext arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean next(List<Cell> arg0, ScannerContext arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}
	
	

}
