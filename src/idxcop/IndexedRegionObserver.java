package idxcop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;

public class IndexedRegionObserver extends BaseRegionObserver {

	@Override
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> ctx) {
		super.postOpen(ctx);
		TableName name = ctx.getEnvironment().getRegion().getTableDesc().getTableName();
		byte[] tableName = name.getName();
		String tableNameStr = Bytes.toString(tableName);
		// if (IndexUtils.isCatalogTable(tableName) ||
		// IndexUtils.isIndexTable(tableNameStr)) {
		// return;
		// }

		// this.indexManager.incrementRegionCount(tableNameStr);
		// List<IndexSpecification> list =
		// indexManager.getIndicesForTable(tableNameStr);
		// if (null != list) {
		// return;
		// }
		RegionServerServices rss = ctx.getEnvironment().getRegionServerServices();
		Configuration conf = rss.getConfiguration();
		// IndexedHTableDescriptor tableDescriptor = null;
		// try {
		// tableDescriptor = IndexUtils.getIndexedHTableDescriptor(tableName,
		// conf);
		// } catch (IOException e) {
		// rss.abort("Some unidentified scenario while reading from the "
		// + "table descriptor . Aborting RegionServer", e);
		// }
		// if (tableDescriptor != null) {
		// list = tableDescriptor.getIndices();
		// if (list != null && list.size() > 0) {
		// indexManager.addIndexForTable(tableNameStr, list);
		// LOG.trace("Added index Specification in the Manager for the " +
		// tableNameStr);
		// } else {
		// list = new ArrayList<IndexSpecification>();
		// indexManager.addIndexForTable(tableNameStr, list);
		// LOG.trace("Added index Specification in the Manager for the " +
		// tableNameStr);
		// }
		// }
		// LOG.trace("Exiting postOpen for the table " + tableNameStr);
	};

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> ctx, Get get, List<Cell> results)
			throws IOException {
		// TODO Auto-generated method stub
		super.preGetOp(ctx, get, results);

	}

	@Override
	public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> ctx,
			MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
		// TODO Auto-generated method stub
		super.preBatchMutate(ctx, miniBatchOp);

		HRegionServer rs = (HRegionServer) ctx.getEnvironment().getRegionServerServices();
		HRegion userRegion = (HRegion) ctx.getEnvironment().getRegion();
		HTableDescriptor userTableDesc = userRegion.getTableDesc();
		String tableName = userTableDesc.getNameAsString();
		// if (IndexUtils.isCatalogTable(userTableDesc.getName()) ||
		// IndexUtils.isIndexTable(tableName)) {
		// return;
		// }
		// List<IndexSpecification> indices =
		// indexManager.getIndicesForTable(tableName);
		// if (indices == null || indices.isEmpty()) {
		// LOG.trace("skipping preBatchMutate for the table " + tableName + " as
		// there are no indices");
		// return;
		// }
		// LOG.trace("Entering preBatchMutate for the table " + tableName);
		// LOG.trace("Indices for the table " + tableName + " are: " + indices);
		// HRegion indexRegion = getIndexTableRegion(tableName, userRegion, rs);
		// // Storing this found HRegion in the index table within the thread
		// locale.
		// IndexEdits indexEdits = threadLocal.get();
		// indexEdits.indexRegion = indexRegion;
		// for (Pair<Mutation, OperationStatus> mutation : mutationVsBatchOp) {
		// if (mutation.getSecond().getOperationStatusCode() !=
		// OperationStatusCode.NOT_RUN) {
		// continue;
		// }
		// // only for successful puts
		// Mutation m = mutation.getFirst();
		// if (m instanceof Put) {
		// try {
		// prepareIndexMutations(indices, userRegion, m, tableName,
		// indexRegion);
		// } catch (IOException e) {
		// mutation.setSecond(new
		// OperationStatus(OperationStatusCode.SANITY_CHECK_FAILURE, e
		// .getMessage()));
		// }
		// } else if (m instanceof Delete) {
		// prepareIndexMutations(indices, userRegion, m, tableName,
		// indexRegion);
		// }
		// }
		// indexEdits.setUpdateLocked();
		// indexRegion.updateLock();
		// LOG.trace("Exiting preBatchMutate for the table " + tableName);
	}

	@Override
	public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> ctx,
			MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
		super.postBatchMutate(ctx, miniBatchOp);

		HTableDescriptor userTableDesc = ctx.getEnvironment().getRegion().getTableDesc();
		String tableName = userTableDesc.getNameAsString();
		// if (IndexUtils.isCatalogTable(userTableDesc.getName()) ||
		// IndexUtils.isIndexTable(tableName)) {
		// return;
		// }
		// List<IndexSpecification> indices =
		// indexManager.getIndicesForTable(tableName);
		// if (indices == null || indices.isEmpty()) {
		// LOG.trace("skipping postBatchMutate for the table " + tableName + "
		// as there are no indices");
		// return;
		// }
		// LOG.trace("Entering postBatchMutate for the table " + tableName);
		// IndexEdits indexEdits = threadLocal.get();
		// List<Pair<Mutation, Integer>> indexMutations =
		// indexEdits.getIndexMutations();
		//
		// if (indexMutations.size() == 0) {
		// return;
		// }
		// HRegion hr = indexEdits.getRegion();
		// LOG.trace("Updating index table " +
		// hr.getRegionInfo().getTableNameAsString());
		// try {
		// hr.batchMutateForIndex(indexMutations.toArray(new
		// Pair[indexMutations.size()]));
		// } catch (IOException e) {
		// // TODO This can come? If so we need to revert the actual put
		// // and make the op failed.
		// LOG.error("Error putting data into the index region", e);
		// }
		// LOG.trace("Exiting postBatchMutate for the table " + tableName);
	}

	@Override
	public boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> ctx, InternalScanner s,
			byte[] currentRow, int offset, short length, boolean hasMore) throws IOException {
		return super.postScannerFilterRow(ctx, s, currentRow, offset, length, hasMore);
		// String tableName =
		// e.getEnvironment().getRegion().getTableDesc().getNameAsString();
		// if (IndexUtils.isIndexTable(tableName)) {
		// return true;
		// }
		// SeekAndReadRegionScanner bsrs =
		// SeekAndReadRegionScannerHolder.getRegionScanner();
		// if (bsrs != null) {
		// while (false == bsrs.seekToNextPoint()) {
		// SeekPointFetcher seekPointFetcher = scannerMap.get(bsrs);
		// if (null != seekPointFetcher) {
		// List<byte[]> seekPoints = new ArrayList<byte[]>(1);
		// seekPointFetcher.nextSeekPoints(seekPoints, 1);
		// // TODO use return boolean?
		// if (seekPoints.isEmpty()) {
		// LOG.trace("No seekpoints are remaining hence returning.. ");
		// return false;
		// }
		// bsrs.addSeekPoints(seekPoints);
		// if (isTestingEnabled) {
		// setSeekPoints(seekPoints);
		// setSeekpointAdded(true);
		// addSeekPoints(seekPoints);
		// }
		// } else {
		// // This will happen for a region with no index
		// break;
		// }
		// }
		// }
		// return true;

	}

	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Scan scan, RegionScanner s)
			throws IOException {
		return super.postScannerOpen(ctx, scan, s);

		// HRegion region = (HRegion) env.getEnvironment().getRegion();
		// String tableName = region.getTableDesc().getNameAsString();
		// HRegionServer rs = (HRegionServer)
		// env.getEnvironment().getRegionServerServices();
		// If the passed region is a region from an indexed table
		// SeekAndReadRegionScanner bsrs = null;
		//
		// try {
		// List<IndexSpecification> indexlist =
		// IndexManager.getInstance().getIndicesForTable(tableName);
		// if (indexlist != null) {
		// if (indexlist == null || indexlist.isEmpty()) {
		// // Not an indexed table. Just return.
		// return s;
		// }
		// LOG.trace("Entering postScannerOpen for the table " + tableName);
		// Collection<HRegion> onlineRegions =
		// rs.getOnlineRegionsLocalContext();
		// for (HRegion onlineIdxRegion : onlineRegions) {
		// if
		// (IndexUtils.isCatalogTable(Bytes.toBytes(onlineIdxRegion.getTableDesc()
		// .getNameAsString()))) {
		// continue;
		// }
		// if (onlineIdxRegion.equals(region)) {
		// continue;
		// }
		// if (Bytes.equals(onlineIdxRegion.getStartKey(), region.getStartKey())
		// &&
		// Bytes.equals(Bytes.toBytes(IndexUtils.getIndexTableName(region.getTableDesc()
		// .getNameAsString())), onlineIdxRegion.getTableDesc().getName())) {
		// ScanFilterEvaluator mapper = new ScanFilterEvaluator();
		// IndexRegionScanner indexScanner =
		// mapper.evaluate(scan, indexlist, onlineIdxRegion.getStartKey(),
		// onlineIdxRegion,
		// tableName);
		// if (indexScanner == null) return s;
		// SeekPointFetcher spf = new SeekPointFetcher(indexScanner);
		// ReInitializableRegionScanner reinitializeScanner =
		// new ReInitializableRegionScannerImpl(s, scan, spf);
		// bsrs = new BackwardSeekableRegionScanner(reinitializeScanner, scan,
		// region, null);
		// scannerMap.put(bsrs, spf);
		// LOG.trace("Scanner Map has " + scannerMap);
		// break;
		// }
		// }
		// LOG.trace("Exiting postScannerOpen for the table " + tableName);
		// }
		// } catch (Exception ex) {
		// LOG.error("Exception occured in postScannerOpen for the table " +
		// tableName, ex);
		// }
		// if (bsrs != null) {
		// return bsrs;
		// } else {
		// return s;
		// }
	}

	@Override
	public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> ctx, InternalScanner s,
			List<Result> results, int limit, boolean hasMore) throws IOException {
		return super.preScannerNext(ctx, s, results, limit, hasMore);

		// HRegion region = e.getEnvironment().getRegion();
		// String tableName = region.getTableDesc().getNameAsString();
		// try {
		// if (s instanceof SeekAndReadRegionScanner) {
		// LOG.trace("Entering preScannerNext for the table " + tableName);
		// BackwardSeekableRegionScanner bsrs = (BackwardSeekableRegionScanner)
		// s;
		// SeekAndReadRegionScannerHolder.setRegionScanner(bsrs);
		// SeekPointFetcher spf = scannerMap.get(bsrs);
		// List<byte[]> seekPoints = null;
		// if (spf != null) {
		// if (isTestingEnabled) {
		// setIndexedFlowUsed(true);
		// }
		// seekPoints = new ArrayList<byte[]>();
		// spf.nextSeekPoints(seekPoints, nbRows);
		// }
		// if (seekPoints == null || seekPoints.isEmpty()) {
		// LOG.trace("No seekpoints are remaining hence returning.. ");
		// SeekAndReadRegionScannerHolder.removeRegionScanner();
		// e.bypass();
		// return false;
		// }
		// bsrs.addSeekPoints(seekPoints);
		// // This setting is just for testing purpose
		// if (isTestingEnabled) {
		// setSeekPoints(seekPoints);
		// setSeekpointAdded(true);
		// addSeekPoints(seekPoints);
		// }
		// LOG.trace("Exiting preScannerNext for the table " + tableName);
		// }
		// } catch (Exception ex) {
		// LOG.error("Exception occured in preScannerNext for the table " +
		// tableName + ex);
		// }
		// return true;
	}

	@Override
	public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> ctx, InternalScanner s,
			List<Result> results, int limit, boolean hasMore) throws IOException {
		return super.postScannerNext(ctx, s, results, limit, hasMore);

		// if (s instanceof SeekAndReadRegionScanner) {
		// SeekAndReadRegionScannerHolder.removeRegionScanner();
		// }
		// return true;
	}

	@Override
	public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> ctx, InternalScanner s)
			throws IOException {
		super.preScannerClose(ctx, s);

		// if (s instanceof BackwardSeekableRegionScanner) {
		// scannerMap.remove((RegionScanner) s);
		// }
	}

	@Override
	public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] splitKey,
			List<Mutation> metaEntries) throws IOException {
		super.preSplitBeforePONR(ctx, splitKey, metaEntries);

		// RegionCoprocessorEnvironment environment = e.getEnvironment();
		// HRegionServer rs = (HRegionServer)
		// environment.getRegionServerServices();
		// HRegion region = environment.getRegion();
		// String userTableName = region.getTableDesc().getNameAsString();
		// LOG.trace("Entering preSplitBeforePONR for the table " +
		// userTableName + " for the region "
		// + region.getRegionInfo());
		// String indexTableName = IndexUtils.getIndexTableName(userTableName);
		// if (indexManager.getIndicesForTable(userTableName) != null) {
		// HRegion indexRegion = null;
		// SplitTransaction st = null;
		// try {
		// indexRegion = getIndexRegion(rs, region.getStartKey(),
		// indexTableName);
		// if (null != indexRegion) {
		// LOG.info("Flushing the cache for the index table " + indexTableName +
		// " for the region "
		// + indexRegion.getRegionInfo());
		// indexRegion.flushcache();
		// if (LOG.isInfoEnabled()) {
		// LOG.info("Forcing split for the index table " + indexTableName + "
		// with split key "
		// + Bytes.toString(splitKey));
		// }
		// st = new SplitTransaction(indexRegion, splitKey);
		// if (!st.prepare()) {
		// LOG.error("Prepare for the index table " + indexTableName
		// + " failed. So returning null. ");
		// return null;
		// }
		// indexRegion.forceSplit(splitKey);
		// PairOfSameType<HRegion> daughterRegions =
		// st.stepsBeforeAddingPONR(rs, rs, false);
		// SplitInfo splitInfo = splitThreadLocal.get();
		// splitInfo.setDaughtersAndTransaction(daughterRegions, st);
		// LOG.info("Daughter regions created for the index table " +
		// indexTableName
		// + " for the region " + indexRegion.getRegionInfo());
		// return splitInfo;
		// } else {
		// LOG.error("IndexRegion for the table " + indexTableName + " is null.
		// So returning null. ");
		// return null;
		// }
		// } catch (Exception ex) {
		// LOG.error("Error while spliting the indexTabRegion or not able to get
		// the indexTabRegion:"
		// + indexRegion != null ? indexRegion.getRegionName() : "", ex);
		// st.rollback(rs, rs);
		// return null;
		// }
		// }
		// LOG.trace("Indexes for the table " + userTableName
		// + " are null. So returning the empty SplitInfo");
		// return new SplitInfo();
	}

	@Override
	public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		super.preSplitAfterPONR(ctx);

		// RegionCoprocessorEnvironment environment = ctx.getEnvironment();
		// HRegionServer rs = (HRegionServer)
		// environment.getRegionServerServices();
		// HRegion region = environment.getRegion();
		// String userTableName = region.getTableDesc().getNameAsString();
		// String indexTableName = IndexUtils.getIndexTableName(userTableName);
		// if (IndexUtils.isIndexTable(userTableName)) {
		// return;
		// }
		// LOG.trace("Entering postSplit for the table " + userTableName + " for
		// the region "
		// + region.getRegionInfo());
		// IndexManager indexManager = IndexManager.getInstance();
		// SplitTransaction splitTransaction = null;
		// if (indexManager.getIndicesForTable(userTableName) != null) {
		// try {
		// SplitInfo splitInfo = splitThreadLocal.get();
		// splitTransaction = splitInfo.getSplitTransaction();
		// PairOfSameType<HRegion> daughters = splitInfo.getDaughters();
		// if (splitTransaction != null && daughters != null) {
		// splitTransaction.stepsAfterPONR(rs, rs, daughters);
		// LOG.info("Daughter regions are opened and split transaction finished"
		// + " for zknodes for index table " + indexTableName + " for the region
		// "
		// + region.getRegionInfo());
		// }
		// } catch (Exception ex) {
		// String msg =
		// "Splitting of index region has failed in stepsAfterPONR stage so
		// aborting the server";
		// LOG.error(msg, ex);
		// rs.abort(msg);
		// }
		// }
	}

	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		super.preSplit(ctx);

		// if (splitThreadLocal != null) {
		// splitThreadLocal.remove();
		// splitThreadLocal.set(new SplitInfo());
		// }
	}

}
