package idxcop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.regionserver.RegionScanner;

public interface SeekAndReadRegionScanner extends RegionScanner {

	void addSeekPoints(List<byte[]> seekPoints);

	byte[] getLatestSeekpoint();

	boolean seekToNextPoint() throws IOException;

	public boolean isClosed();
}
