package util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is an extension of the load balancer class. It allows to colocate the regions of the
 * actual table and the regions of the indexed table. roundRobinAssignment, retainAssignment ->
 * index regions will follow the actual table regions. randomAssignment -> either index table or
 * actual table region will follow each other based on which ever comes first. In case of master
 * failover there is a chance that the znodes of the index table and actual table are left behind.
 * Then in that scenario we may get randomAssignment for either the actual table region first or the
 * index table region first.
 */

public class SecIndexLoadBalancer implements LoadBalancer {

  private static final Log LOG = LogFactory.getLog(SecIndexLoadBalancer.class);

  private LoadBalancer delegator;

  private MasterServices master;

  private static final Random RANDOM = new Random(System.currentTimeMillis());

  private Map<String, Map<HRegionInfo, ServerName>> regionLocation =
      new ConcurrentHashMap<String, Map<HRegionInfo, ServerName>>();

  @Override
  public Configuration getConf() {
    return this.delegator.getConf();
  }

  @Override
  public void setConf(Configuration configuration) {
    this.delegator.setConf(configuration);
  }

  @Override
  public void setClusterStatus(ClusterStatus st) {
    this.delegator.setClusterStatus(st);

  }

  public Map<String, Map<HRegionInfo, ServerName>> getRegionLocation() {
    return regionLocation;
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.master = masterServices;
    this.delegator.setMasterServices(masterServices);
  }

  public void setDelegator(LoadBalancer defaultLoadBalancer) {
    this.delegator = defaultLoadBalancer;
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {
    synchronized (this.regionLocation) {
      Map<ServerName, List<HRegionInfo>> userClusterState =
          new HashMap<ServerName, List<HRegionInfo>>(1);
      Map<ServerName, List<HRegionInfo>> indexClusterState =
          new HashMap<ServerName, List<HRegionInfo>>(1);
      boolean balanceByTable =
          this.master.getConfiguration().getBoolean("hbase.master.loadbalance.bytable", true);

      String tableName = null;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Seperating user and index regions of each region server in the cluster.");
      }
      if (balanceByTable) {
        // Check and modify the regionLocation map based on values of cluster state because we will
        // call balancer only when the cluster is in stable state and reliable.
        Map<HRegionInfo, ServerName> regionMap = null;
        for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : clusterState.entrySet()) {
          ServerName sn = serverVsRegionList.getKey();
          List<HRegionInfo> regionInfos = serverVsRegionList.getValue();
          if (regionInfos.isEmpty()) {
            continue;
          }
          // Just get the table name from any one of the values in the regioninfo list
          if (null == tableName) {
            tableName = regionInfos.get(0).getTableNameAsString();
            regionMap = this.regionLocation.get(tableName);
          }
          if (regionMap != null) {
            for (HRegionInfo hri : regionInfos) {
              updateServer(regionMap, sn, hri);
            }
          }
        }
      } else {
        for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : clusterState.entrySet()) {
          ServerName sn = serverVsRegionList.getKey();
          List<HRegionInfo> regionsInfos = serverVsRegionList.getValue();
          List<HRegionInfo> idxRegionsToBeMoved = new ArrayList<HRegionInfo>();
          List<HRegionInfo> uRegionsToBeMoved = new ArrayList<HRegionInfo>();
          for (HRegionInfo hri : regionsInfos) {
            if (hri.isMetaRegion() || hri.isRootRegion()) {
              continue;
            }
            tableName = hri.getTableNameAsString();
            // table name may change every time thats why always need to get table entries.
            Map<HRegionInfo, ServerName> regionMap = this.regionLocation.get(tableName);
            if (regionMap != null) {
              updateServer(regionMap, sn, hri);
            }
            if (tableName.endsWith(Constants.INDEX_TABLE_SUFFIX)) {
              idxRegionsToBeMoved.add(hri);
              continue;
            }
            uRegionsToBeMoved.add(hri);

          }
          // there may be dummy entries here if assignments by table is set
          userClusterState.put(sn, uRegionsToBeMoved);
          indexClusterState.put(sn, idxRegionsToBeMoved);
        }
      }
      /*
       * In case of table wise balancing if balanceCluster called for index table then no user
       * regions available. At that time skip default balancecluster call and get region plan from
       * region location map if exist.
       */
      // TODO : Needs refactoring here
      List<RegionPlan> regionPlanList = null;

      if (balanceByTable && (false == tableName.endsWith(Constants.INDEX_TABLE_SUFFIX))) {
        regionPlanList = this.delegator.balanceCluster(clusterState);
        // regionPlanList is null means skipping balancing.
        if (null == regionPlanList) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("User region plan is null.");
          }
          return null;
        } else {
          saveRegionPlanList(regionPlanList);
          return regionPlanList;
        }
      } else if (balanceByTable && (true == tableName.endsWith(Constants.INDEX_TABLE_SUFFIX))) {
        regionPlanList = new ArrayList<RegionPlan>(1);
        String actualTableName = extractActualTableName(tableName);
        Map<HRegionInfo, ServerName> regionMap = regionLocation.get(actualTableName);
        // no previous region plan for user table.
        if (null == regionMap) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No user table region plans present for index table " + tableName + '.');
          }
          return null;
        }
        for (Entry<HRegionInfo, ServerName> e : regionMap.entrySet()) {
          regionPlanList.add(new RegionPlan(e.getKey(), null, e.getValue()));
        }
        // for preparing the index plan
        List<RegionPlan> indexPlanList = new ArrayList<RegionPlan>(1);
        // copy of region plan to iterate.
        List<RegionPlan> regionPlanListCopy = new ArrayList<RegionPlan>(regionPlanList);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Preparing index region plans from user region plans for table " + tableName
              + ".");
        }
        return prepareIndexPlan(clusterState, indexPlanList, regionPlanListCopy);
      } else {
        regionPlanList = this.delegator.balanceCluster(userClusterState);
        if (null == regionPlanList) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("User region plan is null.");
          }
          regionPlanList = new ArrayList<RegionPlan>(1);
        } else {
          saveRegionPlanList(regionPlanList);
        }
        List<RegionPlan> userRegionPlans = new ArrayList<RegionPlan>(1);

        for (Entry<String, Map<HRegionInfo, ServerName>> tableVsRegions : this.regionLocation
            .entrySet()) {
          Map<HRegionInfo, ServerName> regionMap = regionLocation.get(tableVsRegions.getKey());
          // no previous region plan for user table.
          if (null == regionMap) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("No user table region plans present for index table " + tableName + '.');
            }
          } else {
            for (Entry<HRegionInfo, ServerName> e : regionMap.entrySet()) {
              userRegionPlans.add(new RegionPlan(e.getKey(), null, e.getValue()));
            }
          }
        }
        List<RegionPlan> regionPlanListCopy = new ArrayList<RegionPlan>(userRegionPlans);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Preparing index region plans from user region plans for whole cluster.");
        }
        return prepareIndexPlan(indexClusterState, regionPlanList, regionPlanListCopy);
      }
    }
  }

  private void updateServer(Map<HRegionInfo, ServerName> regionMap, ServerName sn, HRegionInfo hri) {
    ServerName existingServer = regionMap.get(hri);
    if (!sn.equals(existingServer)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("There is a mismatch in the existing server name for the region " + hri
            + ".  Replacing the server " + existingServer + " with " + sn + ".");
      }
      regionMap.put(hri, sn);
    }
  }

  // Creates the index region plan based on the corresponding user region plan
  private List<RegionPlan> prepareIndexPlan(Map<ServerName, List<HRegionInfo>> indexClusterState,
      List<RegionPlan> regionPlanList, List<RegionPlan> regionPlanListCopy) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Entered prepareIndexPlan");
    }
    OUTER_LOOP: for (RegionPlan regionPlan : regionPlanListCopy) {
      HRegionInfo hri = regionPlan.getRegionInfo();

      MIDDLE_LOOP: for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : indexClusterState
          .entrySet()) {
        List<HRegionInfo> indexRegions = serverVsRegionList.getValue();
        ServerName server = serverVsRegionList.getKey();
        if (regionPlan.getDestination().equals(server)) {
          // desination server in the region plan is new and should not be same with this
          // server in index cluster state.thats why skipping regions check in this server
          continue MIDDLE_LOOP;
        }
        String actualTableName = null;

        for (HRegionInfo indexRegionInfo : indexRegions) {
          String indexTableName = indexRegionInfo.getTableNameAsString();
          actualTableName = extractActualTableName(indexTableName);
          if (false == hri.getTableNameAsString().equals(actualTableName)) {
            continue;
          }
          if (0 != Bytes.compareTo(hri.getStartKey(), indexRegionInfo.getStartKey())) {
            continue;
          }
          RegionPlan rp = new RegionPlan(indexRegionInfo, server, regionPlan.getDestination());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Selected server " + regionPlan.getDestination()
                + " as destination for region " + indexRegionInfo.getRegionNameAsString()
                + "from user region plan.");
          }

          putRegionPlan(indexRegionInfo, regionPlan.getDestination());
          regionPlanList.add(rp);
          continue OUTER_LOOP;
        }
      }
    }
    regionPlanListCopy.clear();
    // if no user regions to balance then return newly formed index region plan.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exited prepareIndexPlan");
    }
    return regionPlanList;
  }

  private void saveRegionPlanList(List<RegionPlan> regionPlanList) {
    for (RegionPlan regionPlan : regionPlanList) {
      HRegionInfo hri = regionPlan.getRegionInfo();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Saving region plan of region " + hri.getRegionNameAsString() + '.');
      }
      putRegionPlan(hri, regionPlan.getDestination());
    }
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    List<HRegionInfo> userRegions = new ArrayList<HRegionInfo>(1);
    List<HRegionInfo> indexRegions = new ArrayList<HRegionInfo>(1);
    for (HRegionInfo hri : regions) {
      seperateUserAndIndexRegion(hri, userRegions, indexRegions);
    }
    Map<ServerName, List<HRegionInfo>> bulkPlan = null;
    if (false == userRegions.isEmpty()) {
      bulkPlan = this.delegator.roundRobinAssignment(userRegions, servers);
      if (null == bulkPlan) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No region plan for user regions.");
        }
        return null;
      }
      synchronized (this.regionLocation) {
        savePlan(bulkPlan);
      }
    }
    bulkPlan = prepareIndexRegionPlan(indexRegions, bulkPlan, servers);
    return bulkPlan;
  }

  private void seperateUserAndIndexRegion(HRegionInfo hri, List<HRegionInfo> userRegions,
      List<HRegionInfo> indexRegions) {
    if (hri.getTableNameAsString().endsWith(Constants.INDEX_TABLE_SUFFIX)) {
      indexRegions.add(hri);
      return;
    }
    userRegions.add(hri);
  }

  private String extractActualTableName(String indexTableName) {
    int endIndex = indexTableName.length() - Constants.INDEX_TABLE_SUFFIX.length();
    return indexTableName.substring(0, endIndex);
  }

  private Map<ServerName, List<HRegionInfo>> prepareIndexRegionPlan(List<HRegionInfo> indexRegions,
      Map<ServerName, List<HRegionInfo>> bulkPlan, List<ServerName> servers) {
    if (null != indexRegions && false == indexRegions.isEmpty()) {
      if (null == bulkPlan) {
        bulkPlan = new ConcurrentHashMap<ServerName, List<HRegionInfo>>(1);
      }
      for (HRegionInfo hri : indexRegions) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Preparing region plan for index region " + hri.getRegionNameAsString() + '.');
        }
        ServerName destServer = getDestServerForIdxRegion(hri);
        List<HRegionInfo> destServerRegions = null;
        if (null == destServer) {
          destServer = this.randomAssignment(hri, servers);
        }
        if (null != destServer) {
          destServerRegions = bulkPlan.get(destServer);
          if (null == destServerRegions) {
            destServerRegions = new ArrayList<HRegionInfo>(1);
            bulkPlan.put(destServer, destServerRegions);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Server " + destServer + " selected for region "
                + hri.getRegionNameAsString() + '.');
          }
          destServerRegions.add(hri);
        }
      }
    }
    return bulkPlan;
  }

  private ServerName getDestServerForIdxRegion(HRegionInfo hri) {
    // Every time we calculate the table name because in case of master restart the index regions
    // may be coming for different index tables.
    String indexTableName = hri.getTableNameAsString();
    String actualTableName = extractActualTableName(indexTableName);
    synchronized (this.regionLocation) {
      Map<HRegionInfo, ServerName> regionMap = regionLocation.get(actualTableName);
      if (null == regionMap) {
        // Can this case come
        return null;
      }
      for (Map.Entry<HRegionInfo, ServerName> e : regionMap.entrySet()) {
        HRegionInfo uHri = e.getKey();
        if (0 == Bytes.compareTo(uHri.getStartKey(), hri.getStartKey())) {
          // put index region location if corresponding user region found in regionLocation map.
          putRegionPlan(hri, e.getValue());
          return e.getValue();
        }
      }
    }
    return null;
  }

  private void savePlan(Map<ServerName, List<HRegionInfo>> bulkPlan) {
    for (Entry<ServerName, List<HRegionInfo>> e : bulkPlan.entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Saving user regions' plans for server " + e.getKey() + '.');
      }
      for (HRegionInfo hri : e.getValue()) {
        putRegionPlan(hri, e.getKey());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Saved user regions' plans for server " + e.getKey() + '.');
      }
    }
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions,
      List<ServerName> servers) {

    Map<HRegionInfo, ServerName> userRegionsMap = new ConcurrentHashMap<HRegionInfo, ServerName>(1);
    List<HRegionInfo> indexRegions = new ArrayList<HRegionInfo>(1);
    for (Entry<HRegionInfo, ServerName> e : regions.entrySet()) {
      seperateUserAndIndexRegion(e, userRegionsMap, indexRegions, servers);
    }
    Map<ServerName, List<HRegionInfo>> bulkPlan = null;
    if (false == userRegionsMap.isEmpty()) {
      bulkPlan = this.delegator.retainAssignment(userRegionsMap, servers);
      if (null == bulkPlan) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Empty region plan for user regions.");
        }
        return null;
      }
      synchronized (this.regionLocation) {
        savePlan(bulkPlan);
      }
    }
    bulkPlan = prepareIndexRegionPlan(indexRegions, bulkPlan, servers);
    return bulkPlan;
  }

  private void seperateUserAndIndexRegion(Entry<HRegionInfo, ServerName> e,
      Map<HRegionInfo, ServerName> userRegionsMap, List<HRegionInfo> indexRegions,
      List<ServerName> servers) {
    HRegionInfo hri = e.getKey();
    if (hri.getTableNameAsString().endsWith(Constants.INDEX_TABLE_SUFFIX)) {
      indexRegions.add(hri);
      return;
    }
    if (e.getValue() == null) {
      userRegionsMap.put(hri, servers.get(RANDOM.nextInt(servers.size())));
    } else {
      userRegionsMap.put(hri, e.getValue());
    }
  }

  @Override
  public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    return this.delegator.immediateAssignment(regions, servers);
  }

  @Override
  public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers) {
    if (regionInfo.isMetaTable()) {
      // if the region is root or meta table region no need to check for any region plan.
      return this.delegator.randomAssignment(regionInfo, servers);
    }
    ServerName sn = null;
    try {
      sn = getServerNameFromMap(regionInfo, servers);
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not able to get server name.", e);
      }
    } catch (InterruptedException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Interrupted while getting region and location details.", e);
      }
    }
    if (sn == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No server found for region " + regionInfo.getRegionNameAsString() + '.');
      }
      sn = getRandomServer(regionInfo, servers);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Destination server for region " + regionInfo.getRegionNameAsString() + " is "
          + ((sn == null) ? "null" : sn.toString()) + '.');
    }
    return sn;
  }

  private ServerName getRandomServer(HRegionInfo regionInfo, List<ServerName> servers) {
    ServerName sn = null;
    String tableName = regionInfo.getTableNameAsString();
    if (true == IndexUtils.isIndexTable(tableName)) {
      String actualTableName = extractActualTableName(tableName);
      sn =
          this.delegator.randomAssignment(new HRegionInfo(Bytes.toBytes(actualTableName),
              regionInfo.getStartKey(), regionInfo.getEndKey()), servers);
    } else {
      sn = this.delegator.randomAssignment(regionInfo, servers);
    }
    if (sn == null) {
      return null;
    }
    synchronized (this.regionLocation) {
      putRegionPlan(regionInfo, sn);
    }
    return sn;
  }

  private ServerName getServerNameFromMap(HRegionInfo regionInfo, List<ServerName> onlineServers)
      throws IOException, InterruptedException {
    String tableNameOfCurrentRegion = regionInfo.getTableNameAsString();
    String correspondingTableName = null;
    if (false == tableNameOfCurrentRegion.endsWith(Constants.INDEX_TABLE_SUFFIX)) {
      // if the region is user region need to check whether index region plan available or not.
      correspondingTableName = tableNameOfCurrentRegion + Constants.INDEX_TABLE_SUFFIX;
    } else {
      // if the region is index region need to check whether user region plan available or not.
      correspondingTableName = extractActualTableName(tableNameOfCurrentRegion);
    }
    synchronized (this.regionLocation) {

      // skip if its in both index and user and same server
      // I will always have the regionMapWithServerLocation for the correspondingTableName already
      // populated.
      // Only on the first time both the regionMapWithServerLocation and actualRegionMap may be
      // null.
      Map<HRegionInfo, ServerName> regionMapWithServerLocation =
          this.regionLocation.get(correspondingTableName);
      Map<HRegionInfo, ServerName> actualRegionMap =
          this.regionLocation.get(tableNameOfCurrentRegion);

      if (null != regionMapWithServerLocation) {
        for (Entry<HRegionInfo, ServerName> iHri : regionMapWithServerLocation.entrySet()) {
          if (0 == Bytes.compareTo(iHri.getKey().getStartKey(), regionInfo.getStartKey())) {
            ServerName previousServer = null;
            if (null != actualRegionMap) {
              previousServer = actualRegionMap.get(regionInfo);
            }
            ServerName sn = iHri.getValue();
            if (null != previousServer) {
              // if servername of index region and user region are same in regionLocation clean
              // previous plans and return null
              if (previousServer.equals(sn)) {
                regionMapWithServerLocation.remove(iHri.getKey());
                actualRegionMap.remove(regionInfo);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Both user region plan and index region plan "
                      + "in regionLocation are same for the region."
                      + regionInfo.getRegionNameAsString() + " The location is " + sn
                      + ". Hence clearing from regionLocation.");
                }
                return null;
              }
            }
            if (sn != null && onlineServers.contains(sn)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Updating the region " + regionInfo.getRegionNameAsString()
                    + " with server " + sn);
              }
              putRegionPlan(regionInfo, sn);
              return sn;
            } else if (sn != null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("The location " + sn + " of region "
                    + iHri.getKey().getRegionNameAsString()
                    + " is not in online. Selecting other region server.");
              }
              return null;
            }
          }
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No region plans in regionLocation for table " + correspondingTableName);
        }
      }
      return null;
    }
  }

  public void putRegionPlan(HRegionInfo regionInfo, ServerName sn) {
    String tableName = regionInfo.getTableNameAsString();
    synchronized (this.regionLocation) {
      Map<HRegionInfo, ServerName> regionMap = this.regionLocation.get(tableName);
      if (null == regionMap) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No regions of table " + tableName + " in the region plan.");
        }
        regionMap = new ConcurrentHashMap<HRegionInfo, ServerName>(1);
        this.regionLocation.put(tableName, regionMap);
      }
      regionMap.put(regionInfo, sn);
    }
  }

  public void clearTableRegionPlans(String tableName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Clearing regions plans from regionLocation for table " + tableName);
    }
    synchronized (this.regionLocation) {
      this.regionLocation.remove(tableName);
    }
  }

  public void clearRegionInfoFromRegionPlan(HRegionInfo regionInfo) {
    String tableName = regionInfo.getTableNameAsString();
    synchronized (this.regionLocation) {
      Map<HRegionInfo, ServerName> regionMap = this.regionLocation.get(tableName);
      if (null == regionMap) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No regions of table " + tableName + " in the region plan.");
        }
      } else {
        regionMap.remove(regionInfo);
        if (LOG.isDebugEnabled()) {
          LOG.debug("The regioninfo " + regionInfo + " removed from the region plan");
        }
      }
    }
  }

}
