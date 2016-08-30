package idxcop;

import java.io.IOException;

import org.apache.hadoop.hbase.regionserver.RegionScanner;

public interface ReInitializableRegionScanner extends SeekAndReadRegionScanner {

	// TODO better name
	void reInit(RegionScanner rs) throws IOException;

}
