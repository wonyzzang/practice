package idxcop;

import org.apache.hadoop.hbase.regionserver.RegionScanner;

public interface IndexRegionScanner extends RegionScanner{
	
	public void advance();

	public void setRangeFlag(boolean range);

	public boolean isRange();

	public void setScannerIndex(int index);

	public int getScannerIndex();

	public boolean hasChildScanners();
}
