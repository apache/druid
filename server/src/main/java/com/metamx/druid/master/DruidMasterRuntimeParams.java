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
import com.metamx.druid.master.rules.RuleMap;
import com.metamx.emitter.service.ServiceEmitter;

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
  private final ServiceEmitter emitter;
  private final long millisToWaitBeforeDeleting;
  private final MasterStats stats;
  private final long mergeBytesLimit;
  private final int mergeSegmentsLimit;

  public DruidMasterRuntimeParams(
      long startTime,
      DruidCluster druidCluster,
      DatabaseRuleManager databaseRuleManager,
      SegmentReplicantLookup segmentReplicantLookup,
      Set<DruidDataSource> dataSources,
      Set<DataSegment> availableSegments,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ServiceEmitter emitter,
      long millisToWaitBeforeDeleting,
      MasterStats stats,
      long mergeBytesLimit,
      int mergeSegmentsLimit
  )
  {
    this.startTime = startTime;
    this.druidCluster = druidCluster;
    this.databaseRuleManager = databaseRuleManager;
    this.segmentReplicantLookup = segmentReplicantLookup;
    this.dataSources = dataSources;
    this.availableSegments = availableSegments;
    this.loadManagementPeons = loadManagementPeons;
    this.emitter = emitter;
    this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
    this.stats = stats;
    this.mergeBytesLimit = mergeBytesLimit;
    this.mergeSegmentsLimit = mergeSegmentsLimit;
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

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public long getMillisToWaitBeforeDeleting()
  {
    return millisToWaitBeforeDeleting;
  }

  public MasterStats getMasterStats()
  {
    return stats;
  }

  public long getMergeBytesLimit()
  {
    return mergeBytesLimit;
  }

  public int getMergeSegmentsLimit()
  {
    return mergeSegmentsLimit;
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
        emitter,
        millisToWaitBeforeDeleting,
        stats,
        mergeBytesLimit,
        mergeSegmentsLimit
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
    private ServiceEmitter emitter;
    private long millisToWaitBeforeDeleting;
    private MasterStats stats;
    private long mergeBytesLimit;
    private int mergeSegmentsLimit;

    Builder()
    {
      this.startTime = 0;
      this.druidCluster = null;
      this.databaseRuleManager = null;
      this.segmentReplicantLookup = null;
      this.dataSources = Sets.newHashSet();
      this.availableSegments = Sets.newTreeSet(Comparators.inverse(DataSegment.bucketMonthComparator()));
      this.loadManagementPeons = Maps.newHashMap();
      this.emitter = null;
      this.millisToWaitBeforeDeleting = 0;
      this.stats = new MasterStats();
      this.mergeBytesLimit = 0;
      this.mergeSegmentsLimit = 0;
    }

    Builder(
        long startTime,
        DruidCluster cluster,
        DatabaseRuleManager databaseRuleManager,
        SegmentReplicantLookup segmentReplicantLookup,
        Set<DruidDataSource> dataSources,
        Set<DataSegment> availableSegments,
        Map<String, LoadQueuePeon> loadManagementPeons,
        ServiceEmitter emitter,
        long millisToWaitBeforeDeleting,
        MasterStats stats,
        long mergeBytesLimit,
        int mergeSegmentsLimit
    )
    {
      this.startTime = startTime;
      this.druidCluster = cluster;
      this.databaseRuleManager = databaseRuleManager;
      this.segmentReplicantLookup = segmentReplicantLookup;
      this.dataSources = dataSources;
      this.availableSegments = availableSegments;
      this.loadManagementPeons = loadManagementPeons;
      this.emitter = emitter;
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      this.stats = stats;
      this.mergeBytesLimit = mergeBytesLimit;
      this.mergeSegmentsLimit = mergeSegmentsLimit;
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
          emitter,
          millisToWaitBeforeDeleting,
          stats,
          mergeBytesLimit,
          mergeSegmentsLimit
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

    public Builder withEmitter(ServiceEmitter emitter)
    {
      this.emitter = emitter;
      return this;
    }

    public Builder withMillisToWaitBeforeDeleting(long millisToWaitBeforeDeleting)
    {
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      return this;
    }

    public Builder withMasterStats(MasterStats stats)
    {
      this.stats.accumulate(stats);
      return this;
    }

    public Builder withMergeBytesLimit(long mergeBytesLimit)
    {
      this.mergeBytesLimit = mergeBytesLimit;
      return this;
    }

    public Builder withMergeSegmentsLimit(int mergeSegmentsLimit)
    {
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      return this;
    }
  }
}
