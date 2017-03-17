/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.DruidDataSource;
import io.druid.metadata.MetadataRuleManager;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 */
public class DruidCoordinatorRuntimeParams
{
  private final long startTime;
  private final DruidCluster druidCluster;
  private final MetadataRuleManager databaseRuleManager;
  private final SegmentReplicantLookup segmentReplicantLookup;
  private final Set<DruidDataSource> dataSources;
  private final Set<DataSegment> availableSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ReplicationThrottler replicationManager;
  private final ServiceEmitter emitter;
  private final CoordinatorDynamicConfig coordinatorDynamicConfig;
  private final CoordinatorStats stats;
  private final DateTime balancerReferenceTimestamp;
  private final BalancerStrategy balancerStrategy;

  private DruidCoordinatorRuntimeParams(
      long startTime,
      DruidCluster druidCluster,
      MetadataRuleManager databaseRuleManager,
      SegmentReplicantLookup segmentReplicantLookup,
      Set<DruidDataSource> dataSources,
      Set<DataSegment> availableSegments,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ReplicationThrottler replicationManager,
      ServiceEmitter emitter,
      CoordinatorDynamicConfig coordinatorDynamicConfig,
      CoordinatorStats stats,
      DateTime balancerReferenceTimestamp,
      BalancerStrategy balancerStrategy
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
    this.coordinatorDynamicConfig = coordinatorDynamicConfig;
    this.stats = stats;
    this.balancerReferenceTimestamp = balancerReferenceTimestamp;
    this.balancerStrategy = balancerStrategy;
  }

  public long getStartTime()
  {
    return startTime;
  }

  public DruidCluster getDruidCluster()
  {
    return druidCluster;
  }

  public MetadataRuleManager getDatabaseRuleManager()
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

  public CoordinatorDynamicConfig getCoordinatorDynamicConfig()
  {
    return coordinatorDynamicConfig;
  }

  public CoordinatorStats getCoordinatorStats()
  {
    return stats;
  }

  public DateTime getBalancerReferenceTimestamp()
  {
    return balancerReferenceTimestamp;
  }

  public BalancerStrategy getBalancerStrategy()
  {
    return balancerStrategy;
  }

  public boolean hasDeletionWaitTimeElapsed()
  {
    return (System.currentTimeMillis() - getStartTime() > coordinatorDynamicConfig.getMillisToWaitBeforeDeleting());
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
        coordinatorDynamicConfig,
        stats,
        balancerReferenceTimestamp,
        balancerStrategy
    );
  }

  public Builder buildFromExistingWithoutAvailableSegments()
  {
    return new Builder(
        startTime,
        druidCluster,
        databaseRuleManager,
        segmentReplicantLookup,
        dataSources,
        Sets.newTreeSet(DruidCoordinator.SEGMENT_COMPARATOR),
        loadManagementPeons,
        replicationManager,
        emitter,
        coordinatorDynamicConfig,
        stats,
        balancerReferenceTimestamp,
        balancerStrategy
    );
  }

  public static class Builder
  {
    private long startTime;
    private DruidCluster druidCluster;
    private MetadataRuleManager databaseRuleManager;
    private SegmentReplicantLookup segmentReplicantLookup;
    private final Set<DruidDataSource> dataSources;
    private final Set<DataSegment> availableSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private ReplicationThrottler replicationManager;
    private ServiceEmitter emitter;
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private CoordinatorStats stats;
    private DateTime balancerReferenceTimestamp;
    private BalancerStrategy balancerStrategy;

    Builder()
    {
      this.startTime = 0;
      this.druidCluster = null;
      this.databaseRuleManager = null;
      this.segmentReplicantLookup = null;
      this.dataSources = Sets.newHashSet();
      this.availableSegments = Sets.newTreeSet(DruidCoordinator.SEGMENT_COMPARATOR);
      this.loadManagementPeons = Maps.newHashMap();
      this.replicationManager = null;
      this.emitter = null;
      this.stats = new CoordinatorStats();
      this.coordinatorDynamicConfig = new CoordinatorDynamicConfig.Builder().build();
      this.balancerReferenceTimestamp = DateTime.now();
    }

    Builder(
        long startTime,
        DruidCluster cluster,
        MetadataRuleManager databaseRuleManager,
        SegmentReplicantLookup segmentReplicantLookup,
        Set<DruidDataSource> dataSources,
        Set<DataSegment> availableSegments,
        Map<String, LoadQueuePeon> loadManagementPeons,
        ReplicationThrottler replicationManager,
        ServiceEmitter emitter,
        CoordinatorDynamicConfig coordinatorDynamicConfig,
        CoordinatorStats stats,
        DateTime balancerReferenceTimestamp,
        BalancerStrategy balancerStrategy
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
      this.coordinatorDynamicConfig = coordinatorDynamicConfig;
      this.stats = stats;
      this.balancerReferenceTimestamp = balancerReferenceTimestamp;
      this.balancerStrategy=balancerStrategy;
    }

    public DruidCoordinatorRuntimeParams build()
    {
      return new DruidCoordinatorRuntimeParams(
          startTime,
          druidCluster,
          databaseRuleManager,
          segmentReplicantLookup,
          dataSources,
          availableSegments,
          loadManagementPeons,
          replicationManager,
          emitter,
          coordinatorDynamicConfig,
          stats,
          balancerReferenceTimestamp,
          balancerStrategy
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

    public Builder withDatabaseRuleManager(MetadataRuleManager databaseRuleManager)
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

    public Builder withCoordinatorStats(CoordinatorStats stats)
    {
      this.stats.accumulate(stats);
      return this;
    }

    public Builder withDynamicConfigs(CoordinatorDynamicConfig configs)
    {
      this.coordinatorDynamicConfig = configs;
      return this;
    }

    public Builder withBalancerReferenceTimestamp(DateTime balancerReferenceTimestamp)
    {
      this.balancerReferenceTimestamp = balancerReferenceTimestamp;
      return this;
    }

    public Builder withBalancerStrategy(BalancerStrategy balancerStrategy)
    {
      this.balancerStrategy=balancerStrategy;
      return this;
    }
  }
}
