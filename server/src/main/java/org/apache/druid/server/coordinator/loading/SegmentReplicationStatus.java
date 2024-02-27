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

import com.google.common.collect.ImmutableMap;
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
    this.replicaCountsInTier = ImmutableMap.copyOf(replicaCountsInTier);

    final Map<SegmentId, SegmentReplicaCount> totalReplicaCounts = new HashMap<>();
    replicaCountsInTier.forEach((segmentId, tierToReplicaCount) -> {
      final SegmentReplicaCount total = new SegmentReplicaCount();
      tierToReplicaCount.values().forEach(total::accumulate);
      totalReplicaCounts.put(segmentId, total);
    });
    this.totalReplicaCounts = ImmutableMap.copyOf(totalReplicaCounts);
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
}
