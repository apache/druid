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
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.loading.SegmentHolder;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.SegmentLoadingConfig;
import org.apache.druid.server.coordinator.loading.SegmentReplicationStatus;
import org.apache.druid.server.coordinator.loading.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;

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
  private final DruidCluster druidCluster;
  private final StrategicSegmentAssigner segmentAssigner;
  private final TreeSet<DataSegment> usedSegmentsNewestFirst;
  private final DataSourcesSnapshot dataSourcesSnapshot;
  private final CoordinatorDynamicConfig coordinatorDynamicConfig;
  private final DruidCompactionConfig compactionConfig;
  private final SegmentLoadingConfig segmentLoadingConfig;
  private final CoordinatorRunStats stats;
  private final BalancerStrategy balancerStrategy;
  private final Set<String> broadcastDatasources;

  private DruidCoordinatorRuntimeParams(
      DruidCluster druidCluster,
      StrategicSegmentAssigner segmentAssigner,
      TreeSet<DataSegment> usedSegmentsNewestFirst,
      DataSourcesSnapshot dataSourcesSnapshot,
      CoordinatorDynamicConfig coordinatorDynamicConfig,
      DruidCompactionConfig compactionConfig,
      SegmentLoadingConfig segmentLoadingConfig,
      CoordinatorRunStats stats,
      BalancerStrategy balancerStrategy,
      Set<String> broadcastDatasources
  )
  {
    this.druidCluster = druidCluster;
    this.segmentAssigner = segmentAssigner;
    this.usedSegmentsNewestFirst = usedSegmentsNewestFirst;
    this.dataSourcesSnapshot = dataSourcesSnapshot;
    this.coordinatorDynamicConfig = coordinatorDynamicConfig;
    this.compactionConfig = compactionConfig;
    this.segmentLoadingConfig = segmentLoadingConfig;
    this.stats = stats;
    this.balancerStrategy = balancerStrategy;
    this.broadcastDatasources = broadcastDatasources;
  }

  public DruidCluster getDruidCluster()
  {
    return druidCluster;
  }

  @Nullable
  public SegmentReplicationStatus getSegmentReplicationStatus()
  {
    return segmentAssigner == null ? null : segmentAssigner.getReplicationStatus();
  }

  @Nullable
  public Set<DataSegment> getBroadcastSegments()
  {
    return segmentAssigner == null ? null : segmentAssigner.getBroadcastSegments();
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

  /**
   * Used segments ordered by {@link SegmentHolder#NEWEST_SEGMENT_FIRST}.
   */
  public TreeSet<DataSegment> getUsedSegmentsNewestFirst()
  {
    return usedSegmentsNewestFirst;
  }

  /**
   * @return true if the given segment is marked as a "used" segment in the
   * metadata store.
   */
  public boolean isUsedSegment(DataSegment segment)
  {
    return usedSegmentsNewestFirst.contains(segment);
  }

  /**
   * Number of used segments in metadata store.
   */
  public int getUsedSegmentCount()
  {
    return usedSegmentsNewestFirst.size();
  }

  public CoordinatorDynamicConfig getCoordinatorDynamicConfig()
  {
    return coordinatorDynamicConfig;
  }

  public DruidCompactionConfig getCompactionConfig()
  {
    return compactionConfig;
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

  public static Builder builder()
  {
    return new Builder();
  }

  public Builder buildFromExisting()
  {
    return new Builder(
        druidCluster,
        segmentAssigner,
        usedSegmentsNewestFirst,
        dataSourcesSnapshot,
        coordinatorDynamicConfig,
        compactionConfig,
        segmentLoadingConfig,
        stats,
        balancerStrategy,
        broadcastDatasources
    );
  }

  public static class Builder
  {
    private DruidCluster druidCluster;
    private SegmentLoadQueueManager loadQueueManager;
    private StrategicSegmentAssigner segmentAssigner;
    private TreeSet<DataSegment> usedSegmentsNewestFirst;
    private DataSourcesSnapshot dataSourcesSnapshot;
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private DruidCompactionConfig compactionConfig;
    private SegmentLoadingConfig segmentLoadingConfig;
    private CoordinatorRunStats stats;
    private BalancerStrategy balancerStrategy;
    private Set<String> broadcastDatasources;

    private Builder()
    {
      this.coordinatorDynamicConfig = CoordinatorDynamicConfig.builder().build();
      this.compactionConfig = DruidCompactionConfig.empty();
      this.broadcastDatasources = Collections.emptySet();
    }

    private Builder(
        DruidCluster cluster,
        StrategicSegmentAssigner segmentAssigner,
        TreeSet<DataSegment> usedSegmentsNewestFirst,
        DataSourcesSnapshot dataSourcesSnapshot,
        CoordinatorDynamicConfig coordinatorDynamicConfig,
        DruidCompactionConfig compactionConfig,
        SegmentLoadingConfig segmentLoadingConfig,
        CoordinatorRunStats stats,
        BalancerStrategy balancerStrategy,
        Set<String> broadcastDatasources
    )
    {
      this.druidCluster = cluster;
      this.segmentAssigner = segmentAssigner;
      this.usedSegmentsNewestFirst = usedSegmentsNewestFirst;
      this.dataSourcesSnapshot = dataSourcesSnapshot;
      this.coordinatorDynamicConfig = coordinatorDynamicConfig;
      this.compactionConfig = compactionConfig;
      this.segmentLoadingConfig = segmentLoadingConfig;
      this.stats = stats;
      this.balancerStrategy = balancerStrategy;
      this.broadcastDatasources = broadcastDatasources;
    }

    public DruidCoordinatorRuntimeParams build()
    {
      Preconditions.checkNotNull(dataSourcesSnapshot);
      Preconditions.checkNotNull(usedSegmentsNewestFirst);

      initStatsIfRequired();
      initSegmentAssignerIfRequired();

      return new DruidCoordinatorRuntimeParams(
          druidCluster,
          segmentAssigner,
          usedSegmentsNewestFirst,
          dataSourcesSnapshot,
          coordinatorDynamicConfig,
          compactionConfig,
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
      Preconditions.checkNotNull(stats);

      if (segmentLoadingConfig == null) {
        segmentLoadingConfig = SegmentLoadingConfig.create(coordinatorDynamicConfig, usedSegmentsNewestFirst.size());
      }

      segmentAssigner = new StrategicSegmentAssigner(
          loadQueueManager,
          druidCluster,
          balancerStrategy,
          segmentLoadingConfig,
          stats
      );
    }

    private static TreeSet<DataSegment> createUsedSegmentsSet(Iterable<DataSegment> usedSegments)
    {
      TreeSet<DataSegment> segmentsSet = new TreeSet<>(SegmentHolder.NEWEST_SEGMENT_FIRST);
      usedSegments.forEach(segmentsSet::add);
      return segmentsSet;
    }

    public Builder withDruidCluster(DruidCluster cluster)
    {
      this.druidCluster = cluster;
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
      this.usedSegmentsNewestFirst = createUsedSegmentsSet(snapshot.iterateAllUsedSegmentsInSnapshot());
      this.dataSourcesSnapshot = snapshot;
      return this;
    }

    public Builder withUsedSegments(DataSegment... usedSegments)
    {
      return withUsedSegments(Arrays.asList(usedSegments));
    }

    public Builder withUsedSegments(Collection<DataSegment> usedSegments)
    {
      this.usedSegmentsNewestFirst = createUsedSegmentsSet(usedSegments);
      this.dataSourcesSnapshot = DataSourcesSnapshot.fromUsedSegments(usedSegments);
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

    public Builder withCompactionConfig(DruidCompactionConfig config)
    {
      this.compactionConfig = config;
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
