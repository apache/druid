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
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 */
public class DruidCoordinatorRuntimeParams
{
  /**
   * Creates a set to be provided to {@link Builder#setUsedSegments(TreeSet)} method from the given {@link
   * Iterable} of segments.
   *
   * Creates a TreeSet sorted in {@link DruidCoordinator#SEGMENT_COMPARATOR_RECENT_FIRST} order and populates it with
   * the segments from the given iterable. The given iterable is iterated exactly once. No special action is taken if
   * duplicate segments are encountered in the iterable.
   */
  public static TreeSet<DataSegment> createUsedSegmentsSet(Iterable<DataSegment> usedSegments)
  {
    TreeSet<DataSegment> segmentsSet = new TreeSet<>(DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST);
    usedSegments.forEach(segmentsSet::add);
    return segmentsSet;
  }

  private final long startTimeNanos;
  private final DruidCluster druidCluster;
  private final MetadataRuleManager databaseRuleManager;
  private final SegmentReplicantLookup segmentReplicantLookup;
  /** dataSource -> VersionedIntervalTimeline[version String, DataSegment] */
  private final Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSourcesWithUsedSegments;
  private final @Nullable TreeSet<DataSegment> usedSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ReplicationThrottler replicationManager;
  private final ServiceEmitter emitter;
  private final CoordinatorDynamicConfig coordinatorDynamicConfig;
  private final CoordinatorCompactionConfig coordinatorCompactionConfig;
  private final CoordinatorStats stats;
  private final DateTime balancerReferenceTimestamp;
  private final BalancerStrategy balancerStrategy;

  private DruidCoordinatorRuntimeParams(
      long startTimeNanos,
      DruidCluster druidCluster,
      MetadataRuleManager databaseRuleManager,
      SegmentReplicantLookup segmentReplicantLookup,
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSourcesWithUsedSegments,
      @Nullable TreeSet<DataSegment> usedSegments,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ReplicationThrottler replicationManager,
      ServiceEmitter emitter,
      CoordinatorDynamicConfig coordinatorDynamicConfig,
      CoordinatorCompactionConfig coordinatorCompactionConfig,
      CoordinatorStats stats,
      DateTime balancerReferenceTimestamp,
      BalancerStrategy balancerStrategy
  )
  {
    this.startTimeNanos = startTimeNanos;
    this.druidCluster = druidCluster;
    this.databaseRuleManager = databaseRuleManager;
    this.segmentReplicantLookup = segmentReplicantLookup;
    this.dataSourcesWithUsedSegments = dataSourcesWithUsedSegments;
    this.usedSegments = usedSegments;
    this.loadManagementPeons = loadManagementPeons;
    this.replicationManager = replicationManager;
    this.emitter = emitter;
    this.coordinatorDynamicConfig = coordinatorDynamicConfig;
    this.coordinatorCompactionConfig = coordinatorCompactionConfig;
    this.stats = stats;
    this.balancerReferenceTimestamp = balancerReferenceTimestamp;
    this.balancerStrategy = balancerStrategy;
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

  public SegmentReplicantLookup getSegmentReplicantLookup()
  {
    return segmentReplicantLookup;
  }

  /**
   * Returns a "dataSource -> VersionedIntervalTimeline[version String, DataSegment]" map with "used" segments.
   */
  public Map<String, VersionedIntervalTimeline<String, DataSegment>> getDataSourcesWithUsedSegments()
  {
    return dataSourcesWithUsedSegments;
  }

  public TreeSet<DataSegment> getUsedSegments()
  {
    Preconditions.checkState(usedSegments != null, "usedSegments must be set");
    return usedSegments;
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

  public CoordinatorCompactionConfig getCoordinatorCompactionConfig()
  {
    return coordinatorCompactionConfig;
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

  public boolean lagSinceCoordinatorStartElapsedBeforeCanMarkAsUnusedOvershadowedSegements()
  {
    long nanosElapsedSinceCoordinatorStart = System.nanoTime() - getStartTimeNanos();
    long lagNanos = TimeUnit.MILLISECONDS.toNanos(
        coordinatorDynamicConfig.getLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments()
    );
    return nanosElapsedSinceCoordinatorStart > lagNanos;
  }

  public static Builder newBuilder()
  {
    return new Builder();
  }

  public Builder buildFromExisting()
  {
    return new Builder(
        startTimeNanos,
        druidCluster,
        databaseRuleManager,
        segmentReplicantLookup,
        dataSourcesWithUsedSegments,
        usedSegments,
        loadManagementPeons,
        replicationManager,
        emitter,
        coordinatorDynamicConfig,
        coordinatorCompactionConfig,
        stats,
        balancerReferenceTimestamp,
        balancerStrategy
    );
  }

  public Builder buildFromExistingWithoutUsedSegments()
  {
    return new Builder(
        startTimeNanos,
        druidCluster,
        databaseRuleManager,
        segmentReplicantLookup,
        dataSourcesWithUsedSegments,
        null, // usedSegments
        loadManagementPeons,
        replicationManager,
        emitter,
        coordinatorDynamicConfig,
        coordinatorCompactionConfig,
        stats,
        balancerReferenceTimestamp,
        balancerStrategy
    );
  }

  public static class Builder
  {
    private @Nullable Long startTimeNanos;
    private DruidCluster druidCluster;
    private MetadataRuleManager databaseRuleManager;
    private SegmentReplicantLookup segmentReplicantLookup;
    private Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSourcesWithUsedSegments;
    private @Nullable TreeSet<DataSegment> usedSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private ReplicationThrottler replicationManager;
    private ServiceEmitter emitter;
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private CoordinatorCompactionConfig coordinatorCompactionConfig;
    private CoordinatorStats stats;
    private DateTime balancerReferenceTimestamp;
    private BalancerStrategy balancerStrategy;

    private Builder()
    {
      this.startTimeNanos = null;
      this.druidCluster = null;
      this.databaseRuleManager = null;
      this.segmentReplicantLookup = null;
      this.dataSourcesWithUsedSegments = new HashMap<>();
      this.usedSegments = null;
      this.loadManagementPeons = new HashMap<>();
      this.replicationManager = null;
      this.emitter = null;
      this.stats = new CoordinatorStats();
      this.coordinatorDynamicConfig = CoordinatorDynamicConfig.builder().build();
      this.coordinatorCompactionConfig = CoordinatorCompactionConfig.empty();
      this.balancerReferenceTimestamp = DateTimes.nowUtc();
    }

    Builder(
        long startTimeNanos,
        DruidCluster cluster,
        MetadataRuleManager databaseRuleManager,
        SegmentReplicantLookup segmentReplicantLookup,
        Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSourcesWithUsedSegments,
        @Nullable TreeSet<DataSegment> usedSegments,
        Map<String, LoadQueuePeon> loadManagementPeons,
        ReplicationThrottler replicationManager,
        ServiceEmitter emitter,
        CoordinatorDynamicConfig coordinatorDynamicConfig,
        CoordinatorCompactionConfig coordinatorCompactionConfig,
        CoordinatorStats stats,
        DateTime balancerReferenceTimestamp,
        BalancerStrategy balancerStrategy
    )
    {
      this.startTimeNanos = startTimeNanos;
      this.druidCluster = cluster;
      this.databaseRuleManager = databaseRuleManager;
      this.segmentReplicantLookup = segmentReplicantLookup;
      this.dataSourcesWithUsedSegments = dataSourcesWithUsedSegments;
      this.usedSegments = usedSegments;
      this.loadManagementPeons = loadManagementPeons;
      this.replicationManager = replicationManager;
      this.emitter = emitter;
      this.coordinatorDynamicConfig = coordinatorDynamicConfig;
      this.coordinatorCompactionConfig = coordinatorCompactionConfig;
      this.stats = stats;
      this.balancerReferenceTimestamp = balancerReferenceTimestamp;
      this.balancerStrategy = balancerStrategy;
    }

    public DruidCoordinatorRuntimeParams build()
    {
      Preconditions.checkNotNull(startTimeNanos, "startTime must be set");
      return new DruidCoordinatorRuntimeParams(
          startTimeNanos,
          druidCluster,
          databaseRuleManager,
          segmentReplicantLookup,
          dataSourcesWithUsedSegments,
          usedSegments,
          loadManagementPeons,
          replicationManager,
          emitter,
          coordinatorDynamicConfig,
          coordinatorCompactionConfig,
          stats,
          balancerReferenceTimestamp,
          balancerStrategy
      );
    }

    public Builder withStartTimeNanos(long startTimeNanos)
    {
      this.startTimeNanos = startTimeNanos;
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

    @VisibleForTesting
    public Builder setDataSourcesWithUsedSegments(
        Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources
    )
    {
      this.dataSourcesWithUsedSegments = dataSources;
      return this;
    }

    Builder withDataSourcesWithUsedSegments(Collection<ImmutableDruidDataSource> dataSourcesWithUsedSegments)
    {
      dataSourcesWithUsedSegments.forEach(
          dataSource -> {
            VersionedIntervalTimeline<String, DataSegment> timeline = this.dataSourcesWithUsedSegments.computeIfAbsent(
                dataSource.getName(),
                k -> new VersionedIntervalTimeline<>(String.CASE_INSENSITIVE_ORDER)
            );

            dataSource.getSegments().forEach(
                usedSegment -> timeline.add(
                    usedSegment.getInterval(),
                    usedSegment.getVersion(),
                    usedSegment.getShardSpec().createChunk(usedSegment)
                )
            );
          }
      );
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
      return setUsedSegments(createUsedSegmentsSet(usedSegments));
    }

    /**
     * Note: unlike {@link #withUsedSegmentsInTest(Collection)}, this method doesn't make a defensive copy of the
     * provided set. The set passed into this method must not be modified afterwards.
     */
    public Builder setUsedSegments(TreeSet<DataSegment> usedSegments)
    {
      //noinspection ObjectEquality
      if (usedSegments.comparator() != DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST) {
        throw new IllegalArgumentException("Expected DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST");
      }
      this.usedSegments = usedSegments;
      return this;
    }

    public Builder withLoadManagementPeons(Map<String, LoadQueuePeon> loadManagementPeonsCollection)
    {
      loadManagementPeons.putAll(loadManagementPeonsCollection);
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

    public Builder withCompactionConfig(CoordinatorCompactionConfig config)
    {
      this.coordinatorCompactionConfig = config;
      return this;
    }

    public Builder withBalancerReferenceTimestamp(DateTime balancerReferenceTimestamp)
    {
      this.balancerReferenceTimestamp = balancerReferenceTimestamp;
      return this;
    }

    public Builder withBalancerStrategy(BalancerStrategy balancerStrategy)
    {
      this.balancerStrategy = balancerStrategy;
      return this;
    }
  }
}
