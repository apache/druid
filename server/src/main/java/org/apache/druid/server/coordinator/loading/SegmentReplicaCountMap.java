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

import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Contains a mapping from {@link ReplicaCountKey} to {@link SegmentReplicaCount}s.
 * <p>
 * Used by the {@link StrategicSegmentAssigner} to make assignment decisions.
 */
public class SegmentReplicaCountMap
{
  private final Map<SegmentId, Map<ReplicaCountKey, SegmentReplicaCount>> replicaCounts = new HashMap<>();

  static SegmentReplicaCountMap create(DruidCluster cluster, Set<String> coordinatingVersions)
  {
    final SegmentReplicaCountMap replicaCountMap = new SegmentReplicaCountMap();
    replicaCountMap.initReplicaCounts(cluster, coordinatingVersions);
    return replicaCountMap;
  }

  private void initReplicaCounts(DruidCluster cluster, Set<String> coordinatingVersions)
  {
    cluster.getManagedHistoricals().forEach(
        (tier, historicals) -> historicals.forEach(
            serverHolder -> {
              final String group = serverHolder.getServer().getMetadata().getDeploymentGroup();
              final ReplicaCountKey key = ReplicaCountKey.from(tier, group, coordinatingVersions);

              // Add segments already loaded on this server
              for (DataSegment segment : serverHolder.getServedSegments()) {
                computeIfAbsent(segment.getId(), key).incrementLoaded();
              }

              // Add segments queued for load, drop or move on this server
              serverHolder.getQueuedSegments().forEach(
                  (segment, state) -> computeIfAbsent(segment.getId(), key)
                      .incrementQueued(state)
              );
            }
        )
    );

    cluster.getBrokers().forEach(broker -> {
      final ImmutableDruidServer server = broker.getServer();
      for (DataSegment segment : server.iterateAllSegments()) {
        computeIfAbsent(segment.getId(), ReplicaCountKey.forTier(server.getTier()))
            .incrementLoadedOnNonHistoricalServer();
      }
    });

    cluster.getRealtimes().forEach(realtime -> {
      final ImmutableDruidServer server = realtime.getServer();
      for (DataSegment segment : server.iterateAllSegments()) {
        computeIfAbsent(segment.getId(), ReplicaCountKey.forTier(server.getTier()))
            .incrementLoadedOnNonHistoricalServer();
      }
    });
  }

  SegmentReplicaCount get(SegmentId segmentId, ReplicaCountKey key)
  {
    SegmentReplicaCount count = replicaCounts.getOrDefault(segmentId, Collections.emptyMap())
                                             .get(key);
    return count == null ? new SegmentReplicaCount() : count;
  }

  SegmentReplicaCount getTotal(SegmentId segmentId)
  {
    final SegmentReplicaCount total = new SegmentReplicaCount();
    replicaCounts.getOrDefault(segmentId, Collections.emptyMap())
                 .values().forEach(total::accumulate);
    return total;
  }

  public SegmentReplicaCount computeIfAbsent(SegmentId segmentId, ReplicaCountKey key)
  {
    return replicaCounts.computeIfAbsent(segmentId, s -> new HashMap<>())
                        .computeIfAbsent(key, t -> new SegmentReplicaCount());
  }

  public SegmentReplicationStatus toReplicationStatus()
  {
    return new SegmentReplicationStatus(replicaCounts);
  }
}
