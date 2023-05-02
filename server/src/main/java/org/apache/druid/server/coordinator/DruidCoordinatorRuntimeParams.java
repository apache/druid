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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.loadqueue.SegmentLoadQueueManager;
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
import java.util.concurrent.TimeUnit;

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

  private final long startTimeNanos;
  private final DruidCluster druidCluster;
  private final MetadataRuleManager databaseRuleManager;
  private final StrategicSegmentAssigner segmentAssigner;
  private final @Nullable TreeSet<DataSegment> usedSegments;
  private final @Nullable DataSourcesSnapshot dataSourcesSnapshot;
  private final ServiceEmitter emitter;
  private final CoordinatorDynamicConfig coordinatorDynamicConfig;
  private final CoordinatorCompactionConfig coordinatorCompactionConfig;
  private final CoordinatorRunStats stats;
  private final BalancerStrategy balancerStrategy;
  private final Set<String> broadcastDatasources;

  private DruidCoordinatorRuntimeParams(
      long startTimeNanos,
      DruidCluster druidCluster,
      MetadataRuleManager databaseRuleManager,
      StrategicSegmentAssigner segmentAssigner,
      @Nullable TreeSet<DataSegment> usedSegments,
      @Nullable DataSourcesSnapshot dataSourcesSnapshot,
      ServiceEmitter emitter,
      CoordinatorDynamicConfig coordinatorDynamicConfig,
      CoordinatorCompactionConfig coordinatorCompactionConfig,
      CoordinatorRunStats stats,
      BalancerStrategy balancerStrategy,
      Set<String> broadcastDatasources
  )
  {
    this.startTimeNanos = startTimeNanos;
    this.druidCluster = druidCluster;
    this.databaseRuleManager = databaseRuleManager;
    this.segmentAssigner = segmentAssigner;
    this.usedSegments = usedSegments;
    this.dataSourcesSnapshot = dataSourcesSnapshot;
    this.emitter = emitter;
    this.coordinatorDynamicConfig = coordinatorDynamicConfig;
    this.coordinatorCompactionConfig = coordinatorCompactionConfig;
    this.stats = stats;
    this.balancerStrategy = balancerStrategy;
    this.broadcastDatasources = broadcastDatasources;
  }

  public long getStartTimeNanos()
  {
    return startTimeNanos;
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
  public SegmentReplicantLookup getSegmentReplicantLookup()
  {
    return segmentAssigner == null ? null : segmentAssigner.getReplicantLookup();
  }

  public StrategicSegmentAssigner getSegmentAssigner()
  {
    return segmentAssigner;
  }

  /**
   * Creates and returns a "dataSource -> VersionedIntervalTimeline[version String, DataSegment]" map with "used"
   * segments.
   */
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

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public CoordinatorDynamicConfig getCoordinatorDynamicConfig()
  {
    return coordinatorDynamicConfig;
  }

  public CoordinatorCompactionConfig getCoordinatorCompactionConfig()
  {
    return coordinatorCompactionConfig;
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

  public boolean coordinatorIsLeadingEnoughTimeToMarkAsUnusedOvershadowedSegements()
  {
    long nanosElapsedSinceCoordinatorStart = System.nanoTime() - getStartTimeNanos();
    long lagNanos = TimeUnit.MILLISECONDS.toNanos(
        coordinatorDynamicConfig.getLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments()
    );
    return nanosElapsedSinceCoordinatorStart > lagNanos;
  }

  public DataSourcesSnapshot getDataSourcesSnapshot()
  {
    Preconditions.checkState(dataSourcesSnapshot != null, "usedSegments or dataSourcesSnapshot must be set");
    return dataSourcesSnapshot;
  }

  public static Builder newBuilder(long startTimeNanos)
  {
    return new Builder(startTimeNanos);
  }

  public Builder buildFromExisting()
  {
    return new Builder(
        startTimeNanos,
        druidCluster,
        databaseRuleManager,
        segmentAssigner,
        usedSegments,
        dataSourcesSnapshot,
        emitter,
        coordinatorDynamicConfig,
        coordinatorCompactionConfig,
        stats,
        balancerStrategy,
        broadcastDatasources
    );
  }

  public static class Builder
  {
    private final long startTimeNanos;
    private DruidCluster druidCluster;
    private MetadataRuleManager databaseRuleManager;
    private SegmentLoadQueueManager loadQueueManager;
    private StrategicSegmentAssigner segmentAssigner;
    private @Nullable TreeSet<DataSegment> usedSegments;
    private @Nullable DataSourcesSnapshot dataSourcesSnapshot;
    private ServiceEmitter emitter;
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private CoordinatorCompactionConfig coordinatorCompactionConfig;
    private CoordinatorRunStats stats;
    private BalancerStrategy balancerStrategy;
    private Set<String> broadcastDatasources;

    private Builder(long startTimeNanos)
    {
      this.startTimeNanos = startTimeNanos;
      this.coordinatorDynamicConfig = CoordinatorDynamicConfig.builder().build();
      this.coordinatorCompactionConfig = CoordinatorCompactionConfig.empty();
      this.broadcastDatasources = Collections.emptySet();
    }

    private Builder(
        long startTimeNanos,
        DruidCluster cluster,
        MetadataRuleManager databaseRuleManager,
        StrategicSegmentAssigner segmentAssigner,
        @Nullable TreeSet<DataSegment> usedSegments,
        @Nullable DataSourcesSnapshot dataSourcesSnapshot,
        ServiceEmitter emitter,
        CoordinatorDynamicConfig coordinatorDynamicConfig,
        CoordinatorCompactionConfig coordinatorCompactionConfig,
        CoordinatorRunStats stats,
        BalancerStrategy balancerStrategy,
        Set<String> broadcastDatasources
    )
    {
      this.startTimeNanos = startTimeNanos;
      this.druidCluster = cluster;
      this.databaseRuleManager = databaseRuleManager;
      this.segmentAssigner = segmentAssigner;
      this.usedSegments = usedSegments;
      this.dataSourcesSnapshot = dataSourcesSnapshot;
      this.emitter = emitter;
      this.coordinatorDynamicConfig = coordinatorDynamicConfig;
      this.coordinatorCompactionConfig = coordinatorCompactionConfig;
      this.stats = stats;
      this.balancerStrategy = balancerStrategy;
      this.broadcastDatasources = broadcastDatasources;
    }

    public DruidCoordinatorRuntimeParams build()
    {
      initStatsIfRequired();
      initSegmentAssignerIfRequired();

      return new DruidCoordinatorRuntimeParams(
          startTimeNanos,
          druidCluster,
          databaseRuleManager,
          segmentAssigner,
          usedSegments,
          dataSourcesSnapshot,
          emitter,
          coordinatorDynamicConfig,
          coordinatorCompactionConfig,
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

    private void initSegmentAssignerIfRequired()
    {
      if (segmentAssigner != null || loadQueueManager == null) {
        return;
      }

      Preconditions.checkNotNull(druidCluster);
      Preconditions.checkNotNull(balancerStrategy);
      Preconditions.checkNotNull(coordinatorDynamicConfig);
      Preconditions.checkNotNull(stats);
      segmentAssigner = new StrategicSegmentAssigner(
          loadQueueManager,
          druidCluster,
          balancerStrategy,
          coordinatorDynamicConfig,
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
     * Sets the {@link SegmentLoadQueueManager} which is used while constructing
     * the {@link StrategicSegmentAssigner} used in the params.
     */
    public Builder withSegmentAssignerUsing(SegmentLoadQueueManager loadQueueManager)
    {
      this.loadQueueManager = loadQueueManager;
      return this;
    }

    public Builder withSnapshotOfDataSourcesWithAllUsedSegments(DataSourcesSnapshot snapshot)
    {
      this.usedSegments = createUsedSegmentsSet(snapshot.iterateAllUsedSegmentsInSnapshot());
      this.dataSourcesSnapshot = snapshot;
      return this;
    }

    /** This method must be used in test code only. */
    @VisibleForTesting
    public Builder withUsedSegmentsInTest(DataSegment... usedSegments)
    {
      return withUsedSegmentsInTest(Arrays.asList(usedSegments));
    }

    /** This method must be used in test code only. */
    @VisibleForTesting
    public Builder withUsedSegmentsInTest(Collection<DataSegment> usedSegments)
    {
      this.usedSegments = createUsedSegmentsSet(usedSegments);
      this.dataSourcesSnapshot = DataSourcesSnapshot.fromUsedSegments(usedSegments, ImmutableMap.of());
      return this;
    }

    public Builder withEmitter(ServiceEmitter emitter)
    {
      this.emitter = emitter;
      return this;
    }

    public Builder withDynamicConfigs(CoordinatorDynamicConfig configs)
    {
      this.coordinatorDynamicConfig = configs;
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
