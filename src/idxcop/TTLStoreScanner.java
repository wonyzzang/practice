package idxcop;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class TTLStoreScanner implements InternalScanner {
	private InternalScanner delegate;
	private Store store;
	private long smallestReadPoint;
	private long earliestTS;
	private ScanType type; // (This should be scan type)
	private static final Log LOG = LogFactory.getLog(TTLStoreScanner.class);

	private TTLExpiryChecker ttlExpiryChecker;
	private String actualTableName;
	private HRegionServer rs;
	private Boolean userRegionAvailable = null;

	public TTLStoreScanner(Store store, long smallestReadPoint, long earliestTS, ScanType type,
			List<? extends KeyValueScanner> scanners, TTLExpiryChecker ttlExpiryChecker, String actualTableName,
			HRegionServer rs) throws IOException {
		this.store = store;
		this.smallestReadPoint = smallestReadPoint;
		this.earliestTS = earliestTS;
		this.type = type;
		Scan scan = new Scan();
		scan.setMaxVersions(store.getFamily().getMaxVersions());
		delegate = new StoreScanner(store, store.getScanInfo(), scan, scanners, type, this.smallestReadPoint,
				this.earliestTS);
		this.ttlExpiryChecker = ttlExpiryChecker;
		this.actualTableName = actualTableName;
		this.rs = rs;
	}

	@Override
	public boolean next(List<KeyValue> results) throws IOException {
		return this.next(results, 1);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit) throws IOException {
		boolean next = this.delegate.next(result, limit);
		// Ideally here i should get only one result(i.e) only one kv
		for (Iterator<KeyValue> iterator = result.iterator(); iterator.hasNext();) {
			KeyValue kv = (KeyValue) iterator.next();
			byte[] indexNameInBytes = formIndexNameFromKV(kv);
			// From the indexname get the TTL
			IndexSpecification index = IndexManager.getInstance().getIndex(this.actualTableName, indexNameInBytes);
			HRegion hRegion = store.getHRegion();
			if (this.type == ScanType.MAJOR_COMPACT) {
				if (this.userRegionAvailable == null) {
					this.userRegionAvailable = isUserTableRegionAvailable(hRegion.getTableDesc().getNameAsString(),
							hRegion, this.rs);
				}
				// If index is null probably index is been dropped through drop
				// index
				// If user region not available it may be due to the reason that
				// the user region has not yet
				// opened but
				// the index region has opened.
				// Its better not to avoid the kv here, and write it during this
				// current compaction.
				// Anyway later compaction will avoid it. May lead to false
				// positives but better than
				// data loss
				if (null == index && userRegionAvailable) {
					// Remove the dropped index from the results
					LOG.info("The index has been removed for the kv " + kv);
					iterator.remove();
					continue;
				}
			}
			if (index != null) {
				boolean ttlExpired = this.ttlExpiryChecker.checkIfTTLExpired(index.getTTL(), kv.getTimestamp());
				if (ttlExpired) {
					result.clear();
					LOG.info("The ttl has expired for the kv " + kv);
					return false;
				}
			}
		}
		return next;
	}

	@Override
	public void close() throws IOException {
		this.delegate.close();
	}

	private byte[] formIndexNameFromKV(KeyValue kv) {
		byte[] rowKey = kv.getRow();
		// First two bytes are going to be the
		ByteArrayBuilder keyBuilder = ByteArrayBuilder.allocate(rowKey.length);
		// Start from 2nd offset because the first 2 bytes corresponds to the
		// rowkeylength
		keyBuilder.put(rowKey, 0, rowKey.length);
		int indexOf = com.google.common.primitives.Bytes.indexOf(keyBuilder.array(), new byte[1]);
		return keyBuilder.array(indexOf + 1, IndexUtils.getMaxIndexNameLength());
	}

	private boolean isUserTableRegionAvailable(String indexTableName, HRegion indexRegion, HRegionServer rs) {
		Collection<HRegion> userRegions = rs.getOnlineRegions(Bytes.toBytes(this.actualTableName));
		for (HRegion userRegion : userRegions) {
			// TODO start key check is enough? May be we can check for the
			// possibility for N-1 Mapping?
			if (Bytes.equals(userRegion.getStartKey(), indexRegion.getStartKey())) {
				return true;
			}
		}
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
