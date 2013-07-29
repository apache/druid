/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.master;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Comparators;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.emitter.service.ServiceEmitter;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 */
public class DruidMasterRuntimeParams
{
  private final long startTime;
  private final DruidCluster druidCluster;
  private final DatabaseRuleManager databaseRuleManager;
  private final SegmentReplicantLookup segmentReplicantLookup;
  private final Set<DruidDataSource> dataSources;
  private final Set<DataSegment> availableSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ReplicationThrottler replicationManager;
  private final ServiceEmitter emitter;
  private final DynamicConfigs dynamicConfigs;
  private final MasterStats stats;
  private final DateTime balancerReferenceTimestamp;

  public DruidMasterRuntimeParams(
      long startTime,
      DruidCluster druidCluster,
      DatabaseRuleManager databaseRuleManager,
      SegmentReplicantLookup segmentReplicantLookup,
      Set<DruidDataSource> dataSources,
      Set<DataSegment> availableSegments,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ReplicationThrottler replicationManager,
      ServiceEmitter emitter,
      DynamicConfigs dynamicConfigs,
      MasterStats stats,
      DateTime balancerReferenceTimestamp
  )
  {
    this.startTime = startTime;
    this.druidCluster = druidCluster;
    this.databaseRuleManager = databaseRuleManager;
    this.segmentReplicantLookup = segmentReplicantLookup;
    this.dataSources = dataSources;
    this.availableSegments = availableSegments;
    this.loadManagementPeons = loadManagementPeons;
    this.replicationManager = replicationManager;
    this.emitter = emitter;
    this.dynamicConfigs = dynamicConfigs;
    this.stats = stats;
    this.balancerReferenceTimestamp = balancerReferenceTimestamp;
  }

  public long getStartTime()
  {
    return startTime;
  }

  public DruidCluster getDruidCluster()
  {
    return druidCluster;
  }

  public DatabaseRuleManager getDatabaseRuleManager()
  {
    return databaseRuleManager;
  }

  public SegmentReplicantLookup getSegmentReplicantLookup()
  {
    return segmentReplicantLookup;
  }

  public Set<DruidDataSource> getDataSources()
  {
    return dataSources;
  }

  public Set<DataSegment> getAvailableSegments()
  {
    return availableSegments;
  }

  public Map<String, LoadQueuePeon> getLoadManagementPeons()
  {
    return loadManagementPeons;
  }

  public ReplicationThrottler getReplicationManager()
  {
    return replicationManager;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public long getMillisToWaitBeforeDeleting()
  {
    return dynamicConfigs.getMillisToWaitBeforeDeleting();
  }

  public MasterStats getMasterStats()
  {
    return stats;
  }

  public long getMergeBytesLimit()
  {
    return dynamicConfigs.getMergeBytesLimit();
  }

  public int getMergeSegmentsLimit()
  {
    return dynamicConfigs.getMergeSegmentsLimit();
  }

  public int getMaxSegmentsToMove()
  {
    return dynamicConfigs.getMaxSegmentsToMove();
  }

  public DateTime getBalancerReferenceTimestamp()
  {
    return balancerReferenceTimestamp;
  }

  public BalancerCostAnalyzer getBalancerCostAnalyzer(DateTime referenceTimestamp)
  {
    return new BalancerCostAnalyzer(referenceTimestamp);
  }

  public boolean hasDeletionWaitTimeElapsed()
  {
    return (System.currentTimeMillis() - getStartTime() > getMillisToWaitBeforeDeleting());
  }

  public static Builder newBuilder()
  {
    return new Builder();
  }

  public Builder buildFromExisting()
  {
    return new Builder(
        startTime,
        druidCluster,
        databaseRuleManager,
        segmentReplicantLookup,
        dataSources,
        availableSegments,
        loadManagementPeons,
        replicationManager,
        emitter,
        dynamicConfigs,
        stats,
        balancerReferenceTimestamp
    );
  }

  public static class Builder
  {
    private long startTime;
    private DruidCluster druidCluster;
    private DatabaseRuleManager databaseRuleManager;
    private SegmentReplicantLookup segmentReplicantLookup;
    private final Set<DruidDataSource> dataSources;
    private final Set<DataSegment> availableSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private ReplicationThrottler replicationManager;
    private ServiceEmitter emitter;
    private DynamicConfigs dynamicConfigs;
    private MasterStats stats;
    private DateTime balancerReferenceTimestamp;

    Builder()
    {
      this.startTime = 0;
      this.druidCluster = null;
      this.databaseRuleManager = null;
      this.segmentReplicantLookup = null;
      this.dataSources = Sets.newHashSet();
      this.availableSegments = Sets.newTreeSet(Comparators.inverse(DataSegment.bucketMonthComparator()));
      this.loadManagementPeons = Maps.newHashMap();
      this.replicationManager = null;
      this.emitter = null;
      this.stats = new MasterStats();
      this.dynamicConfigs = new DynamicConfigs();
      this.balancerReferenceTimestamp = null;
    }

    Builder(
        long startTime,
        DruidCluster cluster,
        DatabaseRuleManager databaseRuleManager,
        SegmentReplicantLookup segmentReplicantLookup,
        Set<DruidDataSource> dataSources,
        Set<DataSegment> availableSegments,
        Map<String, LoadQueuePeon> loadManagementPeons,
        ReplicationThrottler replicationManager,
        ServiceEmitter emitter,
        DynamicConfigs dynamicConfigs,
        MasterStats stats,
        DateTime balancerReferenceTimestamp
    )
    {
      this.startTime = startTime;
      this.druidCluster = cluster;
      this.databaseRuleManager = databaseRuleManager;
      this.segmentReplicantLookup = segmentReplicantLookup;
      this.dataSources = dataSources;
      this.availableSegments = availableSegments;
      this.loadManagementPeons = loadManagementPeons;
      this.replicationManager = replicationManager;
      this.emitter = emitter;
      this.dynamicConfigs=dynamicConfigs;
      this.stats = stats;
      this.balancerReferenceTimestamp = balancerReferenceTimestamp;
    }

    public DruidMasterRuntimeParams build()
    {
      return new DruidMasterRuntimeParams(
          startTime,
          druidCluster,
          databaseRuleManager,
          segmentReplicantLookup,
          dataSources,
          availableSegments,
          loadManagementPeons,
          replicationManager,
          emitter,
          dynamicConfigs,
          stats,
          balancerReferenceTimestamp
      );
    }

    public Builder withStartTime(long time)
    {
      startTime = time;
      return this;
    }

    public Builder withDruidCluster(DruidCluster cluster)
    {
      this.druidCluster = cluster;
      return this;
    }

    public Builder withDatabaseRuleManager(DatabaseRuleManager databaseRuleManager)
    {
      this.databaseRuleManager = databaseRuleManager;
      return this;
    }

    public Builder withSegmentReplicantLookup(SegmentReplicantLookup lookup)
    {
      this.segmentReplicantLookup = lookup;
      return this;
    }

    public Builder withDatasources(Collection<DruidDataSource> dataSourcesCollection)
    {
      dataSources.addAll(Collections.unmodifiableCollection(dataSourcesCollection));
      return this;
    }

    public Builder withAvailableSegments(Collection<DataSegment> availableSegmentsCollection)
    {
      availableSegments.addAll(Collections.unmodifiableCollection(availableSegmentsCollection));
      return this;
    }

    public Builder withLoadManagementPeons(Map<String, LoadQueuePeon> loadManagementPeonsCollection)
    {
      loadManagementPeons.putAll(Collections.unmodifiableMap(loadManagementPeonsCollection));
      return this;
    }

    public Builder withReplicationManager(ReplicationThrottler replicationManager)
    {
      this.replicationManager = replicationManager;
      return this;
    }

    public Builder withEmitter(ServiceEmitter emitter)
    {
      this.emitter = emitter;
      return this;
    }

    public Builder withMasterStats(MasterStats stats)
    {
      this.stats.accumulate(stats);
      return this;
    }

    public Builder withDynamicConfigs(DynamicConfigs configs)
    {
      this.dynamicConfigs = configs;
      return this;
    }

    public Builder withBalancerReferenceTimestamp(DateTime balancerReferenceTimestamp)
    {
      this.balancerReferenceTimestamp = balancerReferenceTimestamp;
      return this;
    }
  }
}
