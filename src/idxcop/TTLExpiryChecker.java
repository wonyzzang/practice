package idxcop;

import org.apache.hadoop.hbase.HConstants;

public class TTLExpiryChecker {

	public boolean checkIfTTLExpired(long ttl, long timestamp) {
		if (ttl == HConstants.FOREVER) {
			return false;
		} else if (ttl == -1) {
			return false;
		} else {
			// second -> ms adjust for user data
			ttl *= 1000;
		}
		if ((System.currentTimeMillis() - timestamp) > ttl) {
			return true;
		}
		return false;
	}
}
