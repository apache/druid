/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.SegmentLoadingConfig;
import org.apache.druid.server.coordinator.loading.SegmentReplicationStatus;
import org.apache.druid.server.coordinator.loading.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 */
public class DruidCoordinatorRuntimeParams
{
  /**
   * Creates a Set to be assigned into {@link Builder#usedSegments} from the given {@link Iterable} of segments.
   *
   * Creates a TreeSet sorted in {@link DruidCoordinator#SEGMENT_COMPARATOR_RECENT_FIRST} order and populates it with
   * the segments from the given iterable. The given iterable is iterated exactly once. No special action is taken if
   * duplicate segments are encountered in the iterable.
   */
  private static TreeSet<DataSegment> createUsedSegmentsSet(Iterable<DataSegment> usedSegments)
  {
    TreeSet<DataSegment> segmentsSet = new TreeSet<>(DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST);
    usedSegments.forEach(segmentsSet::add);
    return segmentsSet;
  }

  private final DateTime coordinatorStartTime;
  private final DruidCluster druidCluster;
  private final MetadataRuleManager databaseRuleManager;
  private final StrategicSegmentAssigner segmentAssigner;
  private final @Nullable TreeSet<DataSegment> usedSegments;
  private final @Nullable DataSourcesSnapshot dataSourcesSnapshot;
  private final CoordinatorDynamicConfig coordinatorDynamicConfig;
  private final CoordinatorCompactionConfig coordinatorCompactionConfig;
  private final SegmentLoadingConfig segmentLoadingConfig;
  private final CoordinatorRunStats stats;
  private final BalancerStrategy balancerStrategy;
  private final Set<String> broadcastDatasources;

  private DruidCoordinatorRuntimeParams(
      DateTime coordinatorStartTime,
      DruidCluster druidCluster,
      MetadataRuleManager databaseRuleManager,
      StrategicSegmentAssigner segmentAssigner,
      @Nullable TreeSet<DataSegment> usedSegments,
      @Nullable DataSourcesSnapshot dataSourcesSnapshot,
      CoordinatorDynamicConfig coordinatorDynamicConfig,
      CoordinatorCompactionConfig coordinatorCompactionConfig,
      SegmentLoadingConfig segmentLoadingConfig,
      CoordinatorRunStats stats,
      BalancerStrategy balancerStrategy,
      Set<String> broadcastDatasources
  )
  {
    this.coordinatorStartTime = coordinatorStartTime;
    this.druidCluster = druidCluster;
    this.databaseRuleManager = databaseRuleManager;
    this.segmentAssigner = segmentAssigner;
    this.usedSegments = usedSegments;
    this.dataSourcesSnapshot = dataSourcesSnapshot;
    this.coordinatorDynamicConfig = coordinatorDynamicConfig;
    this.coordinatorCompactionConfig = coordinatorCompactionConfig;
    this.segmentLoadingConfig = segmentLoadingConfig;
    this.stats = stats;
    this.balancerStrategy = balancerStrategy;
    this.broadcastDatasources = broadcastDatasources;
  }

  public DateTime getCoordinatorStartTime()
  {
    return coordinatorStartTime;
  }

  public DruidCluster getDruidCluster()
  {
    return druidCluster;
  }

  public MetadataRuleManager getDatabaseRuleManager()
  {
    return databaseRuleManager;
  }

  @Nullable
  public SegmentReplicationStatus getSegmentReplicationStatus()
  {
    return segmentAssigner == null ? null : segmentAssigner.getReplicationStatus();
  }

  public StrategicSegmentAssigner getSegmentAssigner()
  {
    return segmentAssigner;
  }

  public Map<String, SegmentTimeline> getUsedSegmentsTimelinesPerDataSource()
  {
    Preconditions.checkState(dataSourcesSnapshot != null, "dataSourcesSnapshot or usedSegments must be set");
    return dataSourcesSnapshot.getUsedSegmentsTimelinesPerDataSource();
  }

  public TreeSet<DataSegment> getUsedSegments()
  {
    Preconditions.checkState(usedSegments != null, "usedSegments or dataSourcesSnapshot must be set");
    return usedSegments;
  }

  public CoordinatorDynamicConfig getCoordinatorDynamicConfig()
  {
    return coordinatorDynamicConfig;
  }

  public CoordinatorCompactionConfig getCoordinatorCompactionConfig()
  {
    return coordinatorCompactionConfig;
  }

  public SegmentLoadingConfig getSegmentLoadingConfig()
  {
    return segmentLoadingConfig;
  }

  public CoordinatorRunStats getCoordinatorStats()
  {
    return stats;
  }

  public BalancerStrategy getBalancerStrategy()
  {
    return balancerStrategy;
  }

  public Set<String> getBroadcastDatasources()
  {
    return broadcastDatasources;
  }

  public DataSourcesSnapshot getDataSourcesSnapshot()
  {
    Preconditions.checkState(dataSourcesSnapshot != null, "usedSegments or dataSourcesSnapshot must be set");
    return dataSourcesSnapshot;
  }

  public static Builder newBuilder(DateTime coordinatorStartTime)
  {
    return new Builder(coordinatorStartTime);
  }

  public Builder buildFromExisting()
  {
    return new Builder(
        coordinatorStartTime,
        druidCluster,
        databaseRuleManager,
        segmentAssigner,
        usedSegments,
        dataSourcesSnapshot,
        coordinatorDynamicConfig,
        coordinatorCompactionConfig,
        segmentLoadingConfig,
        stats,
        balancerStrategy,
        broadcastDatasources
    );
  }

  public static class Builder
  {
    private final DateTime coordinatorStartTime;
    private DruidCluster druidCluster;
    private MetadataRuleManager databaseRuleManager;
    private SegmentLoadQueueManager loadQueueManager;
    private StrategicSegmentAssigner segmentAssigner;
    private @Nullable TreeSet<DataSegment> usedSegments;
    private @Nullable DataSourcesSnapshot dataSourcesSnapshot;
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private CoordinatorCompactionConfig coordinatorCompactionConfig;
    private SegmentLoadingConfig segmentLoadingConfig;
    private CoordinatorRunStats stats;
    private BalancerStrategy balancerStrategy;
    private Set<String> broadcastDatasources;

    private Builder(DateTime coordinatorStartTime)
    {
      this.coordinatorStartTime = coordinatorStartTime;
      this.coordinatorDynamicConfig = CoordinatorDynamicConfig.builder().build();
      this.coordinatorCompactionConfig = CoordinatorCompactionConfig.empty();
      this.broadcastDatasources = Collections.emptySet();
    }

    private Builder(
        DateTime coordinatorStartTime,
        DruidCluster cluster,
        MetadataRuleManager databaseRuleManager,
        StrategicSegmentAssigner segmentAssigner,
        @Nullable TreeSet<DataSegment> usedSegments,
        @Nullable DataSourcesSnapshot dataSourcesSnapshot,
        CoordinatorDynamicConfig coordinatorDynamicConfig,
        CoordinatorCompactionConfig coordinatorCompactionConfig,
        SegmentLoadingConfig segmentLoadingConfig,
        CoordinatorRunStats stats,
        BalancerStrategy balancerStrategy,
        Set<String> broadcastDatasources
    )
    {
      this.coordinatorStartTime = coordinatorStartTime;
      this.druidCluster = cluster;
      this.databaseRuleManager = databaseRuleManager;
      this.segmentAssigner = segmentAssigner;
      this.usedSegments = usedSegments;
      this.dataSourcesSnapshot = dataSourcesSnapshot;
      this.coordinatorDynamicConfig = coordinatorDynamicConfig;
      this.coordinatorCompactionConfig = coordinatorCompactionConfig;
      this.segmentLoadingConfig = segmentLoadingConfig;
      this.stats = stats;
      this.balancerStrategy = balancerStrategy;
      this.broadcastDatasources = broadcastDatasources;
    }

    public DruidCoordinatorRuntimeParams build()
    {
      initStatsIfRequired();
      initSegmentAssignerIfRequired();

      return new DruidCoordinatorRuntimeParams(
          coordinatorStartTime,
          druidCluster,
          databaseRuleManager,
          segmentAssigner,
          usedSegments,
          dataSourcesSnapshot,
          coordinatorDynamicConfig,
          coordinatorCompactionConfig,
          segmentLoadingConfig,
          stats,
          balancerStrategy,
          broadcastDatasources
      );
    }

    private void initStatsIfRequired()
    {
      Map<Dimension, String> debugDimensions =
          coordinatorDynamicConfig == null ? null : coordinatorDynamicConfig.getValidatedDebugDimensions();
      stats = stats == null ? new CoordinatorRunStats(debugDimensions) : stats;
    }

    /**
     * Initializes {@link StrategicSegmentAssigner} used by historical management
     * duties for segment load/drop/move.
     */
    private void initSegmentAssignerIfRequired()
    {
      if (segmentAssigner != null || loadQueueManager == null) {
        return;
      }

      Preconditions.checkNotNull(druidCluster);
      Preconditions.checkNotNull(balancerStrategy);
      Preconditions.checkNotNull(usedSegments);
      Preconditions.checkNotNull(stats);

      if (segmentLoadingConfig == null) {
        segmentLoadingConfig = SegmentLoadingConfig.create(coordinatorDynamicConfig, usedSegments.size());
      }

      segmentAssigner = new StrategicSegmentAssigner(
          loadQueueManager,
          druidCluster,
          balancerStrategy,
          segmentLoadingConfig,
          stats
      );
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

    /**
     * Sets the {@link SegmentLoadQueueManager} which is used to construct the
     * {@link StrategicSegmentAssigner} for this run.
     */
    public Builder withSegmentAssignerUsing(SegmentLoadQueueManager loadQueueManager)
    {
      this.loadQueueManager = loadQueueManager;
      return this;
    }

    public Builder withDataSourcesSnapshot(DataSourcesSnapshot snapshot)
    {
      this.usedSegments = createUsedSegmentsSet(snapshot.iterateAllUsedSegmentsInSnapshot());
      this.dataSourcesSnapshot = snapshot;
      return this;
    }

    public Builder withUsedSegments(DataSegment... usedSegments)
    {
      return withUsedSegments(Arrays.asList(usedSegments));
    }

    public Builder withUsedSegments(Collection<DataSegment> usedSegments)
    {
      this.usedSegments = createUsedSegmentsSet(usedSegments);
      this.dataSourcesSnapshot = DataSourcesSnapshot.fromUsedSegments(usedSegments, ImmutableMap.of());
      return this;
    }

    public Builder withDynamicConfigs(CoordinatorDynamicConfig configs)
    {
      this.coordinatorDynamicConfig = configs;
      return this;
    }

    public Builder withSegmentLoadingConfig(SegmentLoadingConfig config)
    {
      this.segmentLoadingConfig = config;
      return this;
    }

    public Builder withCompactionConfig(CoordinatorCompactionConfig config)
    {
      this.coordinatorCompactionConfig = config;
      return this;
    }

    public Builder withBalancerStrategy(BalancerStrategy balancerStrategy)
    {
      this.balancerStrategy = balancerStrategy;
      return this;
    }

    public Builder withBroadcastDatasources(Set<String> broadcastDatasources)
    {
      this.broadcastDatasources = broadcastDatasources;
      return this;
    }
  }
}
