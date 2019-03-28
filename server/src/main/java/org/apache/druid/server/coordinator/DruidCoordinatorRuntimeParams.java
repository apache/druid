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

/**
 */
public class DruidCoordinatorRuntimeParams
{
  /**
   * Creates a TreeSet sorted in {@link DruidCoordinator#SEGMENT_COMPARATOR_RECENT_FIRST} order and populates it with
   * the segments from the given iterable. The given iterable is iterated exactly once. No special action is taken if
   * duplicate segments are encountered in the iterable.
   */
  public static TreeSet<DataSegment> createAvailableSegmentsSet(Iterable<DataSegment> availableSegments)
  {
    TreeSet<DataSegment> segmentsSet = new TreeSet<>(DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST);
    availableSegments.forEach(segmentsSet::add);
    return segmentsSet;
  }

  private final long startTime;
  private final DruidCluster druidCluster;
  private final MetadataRuleManager databaseRuleManager;
  private final SegmentReplicantLookup segmentReplicantLookup;
  private final Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources;
  private final @Nullable TreeSet<DataSegment> availableSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ReplicationThrottler replicationManager;
  private final ServiceEmitter emitter;
  private final CoordinatorDynamicConfig coordinatorDynamicConfig;
  private final CoordinatorCompactionConfig coordinatorCompactionConfig;
  private final CoordinatorStats stats;
  private final DateTime balancerReferenceTimestamp;
  private final BalancerStrategy balancerStrategy;

  private DruidCoordinatorRuntimeParams(
      long startTime,
      DruidCluster druidCluster,
      MetadataRuleManager databaseRuleManager,
      SegmentReplicantLookup segmentReplicantLookup,
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources,
      @Nullable TreeSet<DataSegment> availableSegments,
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
    this.coordinatorCompactionConfig = coordinatorCompactionConfig;
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

  public Map<String, VersionedIntervalTimeline<String, DataSegment>> getDataSources()
  {
    return dataSources;
  }

  public TreeSet<DataSegment> getAvailableSegments()
  {
    Preconditions.checkState(availableSegments != null, "availableSegments must be set");
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
        coordinatorCompactionConfig,
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
        null, // availableSegments
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
    private long startTime;
    private DruidCluster druidCluster;
    private MetadataRuleManager databaseRuleManager;
    private SegmentReplicantLookup segmentReplicantLookup;
    private Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources;
    private @Nullable TreeSet<DataSegment> availableSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private ReplicationThrottler replicationManager;
    private ServiceEmitter emitter;
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private CoordinatorCompactionConfig coordinatorCompactionConfig;
    private CoordinatorStats stats;
    private DateTime balancerReferenceTimestamp;
    private BalancerStrategy balancerStrategy;

    Builder()
    {
      this.startTime = 0;
      this.druidCluster = null;
      this.databaseRuleManager = null;
      this.segmentReplicantLookup = null;
      this.dataSources = new HashMap<>();
      this.availableSegments = null;
      this.loadManagementPeons = new HashMap<>();
      this.replicationManager = null;
      this.emitter = null;
      this.stats = new CoordinatorStats();
      this.coordinatorDynamicConfig = CoordinatorDynamicConfig.builder().build();
      this.coordinatorCompactionConfig = CoordinatorCompactionConfig.empty();
      this.balancerReferenceTimestamp = DateTimes.nowUtc();
    }

    Builder(
        long startTime,
        DruidCluster cluster,
        MetadataRuleManager databaseRuleManager,
        SegmentReplicantLookup segmentReplicantLookup,
        Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources,
        @Nullable TreeSet<DataSegment> availableSegments,
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
      this.coordinatorCompactionConfig = coordinatorCompactionConfig;
      this.stats = stats;
      this.balancerReferenceTimestamp = balancerReferenceTimestamp;
      this.balancerStrategy = balancerStrategy;
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
          coordinatorCompactionConfig,
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

    public Builder withDataSources(Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources)
    {
      this.dataSources = dataSources;
      return this;
    }

    public Builder withDataSources(Collection<ImmutableDruidDataSource> dataSourcesCollection)
    {
      dataSourcesCollection.forEach(
          dataSource -> {
            VersionedIntervalTimeline<String, DataSegment> timeline = dataSources.computeIfAbsent(
                dataSource.getName(),
                k -> new VersionedIntervalTimeline<>(String.CASE_INSENSITIVE_ORDER)
            );

            dataSource.getSegments().forEach(
                segment -> timeline.add(
                    segment.getInterval(),
                    segment.getVersion(),
                    segment.getShardSpec().createChunk(segment)
                )
            );
          }
      );
      return this;
    }

    /** This method must be used in test code only. */
    @VisibleForTesting
    public Builder withAvailableSegmentsInTest(DataSegment... availableSegments)
    {
      return withAvailableSegmentsInTest(Arrays.asList(availableSegments));
    }

    /** This method must be used in test code only. */
    @VisibleForTesting
    public Builder withAvailableSegmentsInTest(Collection<DataSegment> availableSegments)
    {
      return setAvailableSegments(createAvailableSegmentsSet(availableSegments));
    }

    /**
     * Note: unlike {@link #withAvailableSegmentsInTest(Collection)}, this method doesn't make a defensive copy of the
     * provided set. The set passed into this method must not be modified afterwards.
     */
    public Builder setAvailableSegments(TreeSet<DataSegment> availableSegments)
    {
      //noinspection ObjectEquality
      if (availableSegments.comparator() != DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST) {
        throw new IllegalArgumentException("Expected DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST");
      }
      this.availableSegments = availableSegments;
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
