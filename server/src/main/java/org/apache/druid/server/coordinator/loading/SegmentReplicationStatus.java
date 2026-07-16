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
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.Collections;
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
    // Make a deep copy of replicaCountsInTier as it may be mutated further
    // in the same coordinator cycle (e.g. by BalanceSegments)
    final Map<SegmentId, Map<String, SegmentReplicaCount>> replicaCountsInTierCopy
        = Maps.newHashMapWithExpectedSize(replicaCountsInTier.size());
    final Map<SegmentId, SegmentReplicaCount> totalReplicaCounts
        = Maps.newHashMapWithExpectedSize(replicaCountsInTier.size());
    for (Map.Entry<SegmentId, Map<String, SegmentReplicaCount>> entry : replicaCountsInTier.entrySet()) {
      final Map<String, SegmentReplicaCount> tierCopy = Maps.newHashMapWithExpectedSize(entry.getValue().size());
      final SegmentReplicaCount total = new SegmentReplicaCount();
      for (Map.Entry<String, SegmentReplicaCount> tierEntry : entry.getValue().entrySet()) {
        final SegmentReplicaCount countCopy = tierEntry.getValue().copy();
        tierCopy.put(tierEntry.getKey(), countCopy);
        total.accumulate(countCopy);
      }
      replicaCountsInTierCopy.put(entry.getKey(), Collections.unmodifiableMap(tierCopy));
      totalReplicaCounts.put(entry.getKey(), total);
    }
    this.replicaCountsInTier = Collections.unmodifiableMap(replicaCountsInTierCopy);
    this.totalReplicaCounts = totalReplicaCounts;
  }

  public SegmentReplicaCount getReplicaCountsInCluster(SegmentId segmentId)
  {
    return totalReplicaCounts.get(segmentId);
  }

  /**
   * A segment is unavailable if it has no loaded replicas while still requiring at least one.
   */
  public boolean isUnavailable(SegmentId segmentId)
  {
    final SegmentReplicaCount totalCount = totalReplicaCounts.get(segmentId);
    return totalCount == null || (totalCount.totalLoaded() <= 0 && totalCount.required() != 0);
  }

  /**
   * A segment is deep-storage only if it is not loaded anywhere and requires zero replicas,
   * so it is served exclusively from deep storage.
   */
  public boolean isDeepStorageOnly(SegmentId segmentId)
  {
    final SegmentReplicaCount totalCount = totalReplicaCounts.get(segmentId);
    return totalCount != null && totalCount.totalLoaded() == 0 && totalCount.required() == 0;
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

      // addTo with 0 ensures the datasource is present in the map even when it has no unavailable segments.
      datasourceToUnavailable.addTo(datasource, isUnavailable(segmentId) ? 1 : 0);
      if (isDeepStorageOnly(segmentId)) {
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
}
