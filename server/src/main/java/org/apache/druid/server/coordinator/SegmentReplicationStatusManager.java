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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.apache.druid.server.coordinator.duty.RunRules;
import org.apache.druid.server.coordinator.loading.SegmentReplicaCount;
import org.apache.druid.server.coordinator.loading.SegmentReplicationStatus;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;

/**
 * Manages information about replication status of segments in a cluster.
 */
public class SegmentReplicationStatusManager
{
  private final MetadataManager metadataManager;

  /**
   * Used to determine count of under-replicated or unavailable segments.
   * Updated in each coordinator run in the {@link SegmentReplicationStatusManager.UpdateReplicationStatus} duty.
   * <p>
   * This might have stale information if coordinator runs are delayed. But as
   * long as the {@link SegmentsMetadataManager} has the latest information of
   * used segments, we would only have false negatives and not false positives.
   * In other words, we might report some segments as under-replicated or
   * unavailable even if they are fully replicated. But if a segment is reported
   * as fully replicated, it is guaranteed to be so.
   */
  private volatile SegmentReplicationStatus segmentReplicationStatus = null;

  @Inject
  public SegmentReplicationStatusManager(MetadataManager metadataManager)
  {
    this.metadataManager = metadataManager;
  }

  public Object2IntMap<String> getDatasourceToUnavailableSegmentCount()
  {
    if (segmentReplicationStatus == null) {
      return Object2IntMaps.emptyMap();
    }

    final Object2IntOpenHashMap<String> datasourceToUnavailableSegments = new Object2IntOpenHashMap<>();

    final Iterable<DataSegment> dataSegments = metadataManager.segments().iterateAllUsedSegments();
    for (DataSegment segment : dataSegments) {
      SegmentReplicaCount replicaCount = segmentReplicationStatus.getReplicaCountsInCluster(segment.getId());
      if (replicaCount != null && (replicaCount.totalLoaded() > 0 || replicaCount.required() == 0)) {
        datasourceToUnavailableSegments.addTo(segment.getDataSource(), 0);
      } else {
        datasourceToUnavailableSegments.addTo(segment.getDataSource(), 1);
      }
    }

    return datasourceToUnavailableSegments;
  }

  public Object2IntMap<String> getDatasourceToDeepStorageQueryOnlySegmentCount()
  {
    if (segmentReplicationStatus == null) {
      return Object2IntMaps.emptyMap();
    }

    final Object2IntOpenHashMap<String> datasourceToDeepStorageOnlySegments = new Object2IntOpenHashMap<>();

    final Iterable<DataSegment> dataSegments = metadataManager.segments().iterateAllUsedSegments();
    for (DataSegment segment : dataSegments) {
      SegmentReplicaCount replicaCount = segmentReplicationStatus.getReplicaCountsInCluster(segment.getId());
      if (replicaCount != null && replicaCount.totalLoaded() == 0 && replicaCount.required() == 0) {
        datasourceToDeepStorageOnlySegments.addTo(segment.getDataSource(), 1);
      }
    }

    return datasourceToDeepStorageOnlySegments;
  }

  @Nullable
  public Integer getReplicationFactor(SegmentId segmentId)
  {
    if (segmentReplicationStatus == null) {
      return null;
    }
    SegmentReplicaCount replicaCountsInCluster = segmentReplicationStatus.getReplicaCountsInCluster(segmentId);
    return replicaCountsInCluster == null ? null : replicaCountsInCluster.required();
  }

  private Map<String, Object2LongMap<String>> computeUnderReplicated(
      Iterable<DataSegment> dataSegments,
      boolean computeUsingClusterView
  )
  {
    if (segmentReplicationStatus == null) {
      return Collections.emptyMap();
    } else {
      return segmentReplicationStatus.getTierToDatasourceToUnderReplicated(dataSegments, !computeUsingClusterView);
    }
  }


  public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicatedCount(boolean useClusterView)
  {
    final Iterable<DataSegment> dataSegments = metadataManager.segments().iterateAllUsedSegments();
    return computeUnderReplicated(dataSegments, useClusterView);
  }

  public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicatedCount(
      Iterable<DataSegment> dataSegments,
      boolean useClusterView
  )
  {
    return computeUnderReplicated(dataSegments, useClusterView);
  }

  /**
   * Updates replication status of all used segments. This duty must run after
   * {@link RunRules} so that the number of required replicas for all segments
   * has been determined.
   */
  class UpdateReplicationStatus implements CoordinatorDuty
  {
    @Override
    public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
    {
      segmentReplicationStatus = params.getSegmentReplicationStatus();

      // Collect stats for unavailable and under-replicated segments
      final CoordinatorRunStats stats = params.getCoordinatorStats();
      getDatasourceToUnavailableSegmentCount().forEach(
          (dataSource, numUnavailable) -> stats.add(
              Stats.Segments.UNAVAILABLE,
              RowKey.of(Dimension.DATASOURCE, dataSource),
              numUnavailable
          )
      );
      getTierToDatasourceToUnderReplicatedCount(false).forEach(
          (tier, countsPerDatasource) -> countsPerDatasource.forEach(
              (dataSource, underReplicatedCount) ->
                  stats.addToSegmentStat(Stats.Segments.UNDER_REPLICATED, tier, dataSource, underReplicatedCount)
          )
      );
      getDatasourceToDeepStorageQueryOnlySegmentCount().forEach(
          (dataSource, numDeepStorageOnly) -> stats.add(
              Stats.Segments.DEEP_STORAGE_ONLY,
              RowKey.of(Dimension.DATASOURCE, dataSource),
              numDeepStorageOnly
          )
      );

      return params;
    }
  }
}
