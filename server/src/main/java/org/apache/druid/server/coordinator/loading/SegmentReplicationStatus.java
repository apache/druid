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

package org.apache.druid.server.coordinator.loading;

import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.HashMap;
import java.util.Map;

/**
 * An immutable object that contains information about the under-replicated
 * or unavailable status of all used segments. This state is recomputed by
 * the {@link StrategicSegmentAssigner} in every run.
 */
public class SegmentReplicationStatus
{
  private final Map<SegmentId, SegmentReplicaCount> totalReplicaCounts;
  private final Map<SegmentId, Map<String, SegmentReplicaCount>> replicaCountsInTier;

  public SegmentReplicationStatus(Map<SegmentId, Map<String, SegmentReplicaCount>> replicaCountsInTier)
  {
    // replicaCountsInTier is owned by the caller's SegmentReplicaCountMap, rebuilt fresh each
    // coordinator cycle and not mutated again once this constructor runs, so a defensive deep
    // copy is unnecessary; hold the reference directly and compute totals in the same pass.
    this.replicaCountsInTier = replicaCountsInTier;

    final Map<SegmentId, SegmentReplicaCount> totalReplicaCounts = Maps.newHashMapWithExpectedSize(replicaCountsInTier.size());
    for (Map.Entry<SegmentId, Map<String, SegmentReplicaCount>> entry : replicaCountsInTier.entrySet()) {
      final SegmentReplicaCount total = new SegmentReplicaCount();
      entry.getValue().values().forEach(total::accumulate);
      totalReplicaCounts.put(entry.getKey(), total);
    }
    this.totalReplicaCounts = totalReplicaCounts;
  }

  public SegmentReplicaCount getReplicaCountsInCluster(SegmentId segmentId)
  {
    return totalReplicaCounts.get(segmentId);
  }

  public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicated(
      Iterable<DataSegment> usedSegments,
      boolean ignoreMissingServers
  )
  {
    final Map<String, Object2LongMap<String>> tierToUnderReplicated = new HashMap<>();

    for (DataSegment segment : usedSegments) {
      final Map<String, SegmentReplicaCount> tierToReplicaCount = replicaCountsInTier.get(segment.getId());
      if (tierToReplicaCount == null) {
        continue;
      }

      tierToReplicaCount.forEach((tier, counts) -> {
        final int underReplicated = ignoreMissingServers ? counts.missing() : counts.missingAndLoadable();
        if (underReplicated >= 0) {
          Object2LongOpenHashMap<String> datasourceToUnderReplicated = (Object2LongOpenHashMap<String>)
              tierToUnderReplicated.computeIfAbsent(tier, ds -> new Object2LongOpenHashMap<>());
          datasourceToUnderReplicated.addTo(segment.getDataSource(), underReplicated);
        }
      });
    }

    return tierToUnderReplicated;
  }

  /**
   * Computes unavailable, under-replicated and deep-storage-only segment counts in a single
   * pass over {@code usedSegments}, instead of three independent full iterations. Produces
   * results identical to calling {@link #getReplicaCountsInCluster} and
   * {@link #getTierToDatasourceToUnderReplicated} independently for each segment.
   *
   * @param ignoreMissingServers same semantics as in {@link #getTierToDatasourceToUnderReplicated}.
   */
  public SegmentStatsSnapshot computeSegmentStats(Iterable<DataSegment> usedSegments, boolean ignoreMissingServers)
  {
    final Object2IntOpenHashMap<String> datasourceToUnavailable = new Object2IntOpenHashMap<>();
    final Object2IntOpenHashMap<String> datasourceToDeepStorageOnly = new Object2IntOpenHashMap<>();
    final Map<String, Object2LongMap<String>> tierToUnderReplicated = new HashMap<>();

    for (DataSegment segment : usedSegments) {
      final SegmentId segmentId = segment.getId();
      final String datasource = segment.getDataSource();

      final SegmentReplicaCount totalCount = totalReplicaCounts.get(segmentId);
      if (totalCount != null && (totalCount.totalLoaded() > 0 || totalCount.required() == 0)) {
        datasourceToUnavailable.addTo(datasource, 0);
      } else {
        datasourceToUnavailable.addTo(datasource, 1);
      }
      if (totalCount != null && totalCount.totalLoaded() == 0 && totalCount.required() == 0) {
        datasourceToDeepStorageOnly.addTo(datasource, 1);
      }

      final Map<String, SegmentReplicaCount> tierToReplicaCount = replicaCountsInTier.get(segmentId);
      if (tierToReplicaCount != null) {
        tierToReplicaCount.forEach((tier, counts) -> {
          final int underReplicated = ignoreMissingServers ? counts.missing() : counts.missingAndLoadable();
          if (underReplicated >= 0) {
            Object2LongOpenHashMap<String> datasourceToUnderReplicated = (Object2LongOpenHashMap<String>)
                tierToUnderReplicated.computeIfAbsent(tier, ds -> new Object2LongOpenHashMap<>());
            datasourceToUnderReplicated.addTo(datasource, underReplicated);
          }
        });
      }
    }

    return new SegmentStatsSnapshot(datasourceToUnavailable, tierToUnderReplicated, datasourceToDeepStorageOnly);
  }

  /**
   * Holder for the three segment-stat views computed together by {@link #computeSegmentStats}.
   */
  public static class SegmentStatsSnapshot
  {
    private final Object2IntMap<String> datasourceToUnavailableCount;
    private final Map<String, Object2LongMap<String>> tierToDatasourceToUnderReplicatedCount;
    private final Object2IntMap<String> datasourceToDeepStorageOnlyCount;

    SegmentStatsSnapshot(
        Object2IntMap<String> datasourceToUnavailableCount,
        Map<String, Object2LongMap<String>> tierToDatasourceToUnderReplicatedCount,
        Object2IntMap<String> datasourceToDeepStorageOnlyCount
    )
    {
      this.datasourceToUnavailableCount = datasourceToUnavailableCount;
      this.tierToDatasourceToUnderReplicatedCount = tierToDatasourceToUnderReplicatedCount;
      this.datasourceToDeepStorageOnlyCount = datasourceToDeepStorageOnlyCount;
    }

    public Object2IntMap<String> getDatasourceToUnavailableCount()
    {
      return datasourceToUnavailableCount;
    }

    public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicatedCount()
    {
      return tierToDatasourceToUnderReplicatedCount;
    }

    public Object2IntMap<String> getDatasourceToDeepStorageOnlyCount()
    {
      return datasourceToDeepStorageOnlyCount;
    }
  }
}
