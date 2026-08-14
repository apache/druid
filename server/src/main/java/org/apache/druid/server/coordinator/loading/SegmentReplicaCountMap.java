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

import com.google.common.collect.Sets;
import org.apache.druid.client.DataSegmentAndLoadProfile;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Contains a mapping from tier to {@link SegmentReplicaCount}s.
 * <p>
 * Used by the {@link StrategicSegmentAssigner} to make assignment decisions.
 */
public class SegmentReplicaCountMap
{
  private final Map<SegmentId, Map<String, SegmentReplicaCount>> replicaCounts = new HashMap<>();

  static SegmentReplicaCountMap create(DruidCluster cluster)
  {
    final SegmentReplicaCountMap replicaCountMap = new SegmentReplicaCountMap();
    replicaCountMap.initReplicaCounts(cluster);
    return replicaCountMap;
  }

  private void initReplicaCounts(DruidCluster cluster)
  {
    cluster.getManagedHistoricals().forEach(
        (tier, historicals) -> historicals.forEach(
            serverHolder -> {
              // Add segments already loaded on this server.
              final Collection<DataSegment> servedSegments = serverHolder.getServedSegments();
              final Set<SegmentId> partiallyLoadedIds = Sets.newHashSetWithExpectedSize(servedSegments.size());
              for (DataSegment segment : servedSegments) {
                final SegmentReplicaCount replicaCount = computeIfAbsent(segment.getId(), tier);
                if (DataSegmentAndLoadProfile.profileOf(segment) == null) {
                  replicaCount.incrementLoaded();
                } else {
                  partiallyLoadedIds.add(segment.getId());
                  replicaCount.incrementLoadedWithPartialProfile();
                }
              }

              // Add segments queued for load, drop or move on this server
              serverHolder.getQueuedSegments().forEach(
                  (segment, state) -> {
                    if (isPartialLoadRevert(serverHolder, segment, state, partiallyLoadedIds)) {
                      return;
                    }
                    computeIfAbsent(segment.getId(), tier).incrementQueued(state);
                  }
              );
            }
        )
    );

    cluster.getBrokers().forEach(broker -> {
      final ImmutableDruidServer server = broker.getServer();
      for (DataSegment segment : server.iterateAllSegments()) {
        computeIfAbsent(segment.getId(), server.getTier())
            .incrementLoadedOnNonHistoricalServer();
      }
    });

    cluster.getRealtimes().forEach(realtime -> {
      final ImmutableDruidServer server = realtime.getServer();
      for (DataSegment segment : server.iterateAllSegments()) {
        computeIfAbsent(segment.getId(), server.getTier())
            .incrementLoadedOnNonHistoricalServer();
      }
    });
  }

  /**
   * Whether a queued operation is the in-place reload that
   * {@link StrategicSegmentAssigner#revertPartialProfileReplica} queues to release a partial-load rule that no longer
   * applies. Such a reload refreshes an existing replica rather than adding one, so counting it as {@code loading}
   * would push {@code projectedReplicas} past the requirement; {@code updateReplicasInTier} would then "correct" the
   * phantom surplus by canceling the very reload it queued last run, and requeue it in the same run, churning
   * forever against a backlogged peon.
   */
  private static boolean isPartialLoadRevert(
      ServerHolder serverHolder,
      DataSegment segment,
      SegmentAction state,
      Set<SegmentId> partiallyLoadedIds
  )
  {
    return state == SegmentAction.LOAD
           && partiallyLoadedIds.contains(segment.getId())
           && serverHolder.getInFlightProfile(segment) == null;
  }

  SegmentReplicaCount get(SegmentId segmentId, String tier)
  {
    SegmentReplicaCount count = replicaCounts.getOrDefault(segmentId, Collections.emptyMap())
                                             .get(tier);
    return count == null ? new SegmentReplicaCount() : count;
  }

  SegmentReplicaCount getTotal(SegmentId segmentId)
  {
    final SegmentReplicaCount total = new SegmentReplicaCount();
    replicaCounts.getOrDefault(segmentId, Collections.emptyMap())
                 .values().forEach(total::accumulate);
    return total;
  }

  public SegmentReplicaCount computeIfAbsent(SegmentId segmentId, String tier)
  {
    return replicaCounts.computeIfAbsent(segmentId, s -> new HashMap<>())
                        .computeIfAbsent(tier, t -> new SegmentReplicaCount());
  }

  public SegmentReplicationStatus toReplicationStatus()
  {
    return new SegmentReplicationStatus(replicaCounts);
  }
}
