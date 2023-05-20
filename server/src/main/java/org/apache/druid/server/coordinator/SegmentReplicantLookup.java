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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.server.coordinator.loadqueue.SegmentAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.HashMap;
import java.util.Map;

/**
 * A lookup for the number of replicants of a given segment for a certain tier.
 */
public class SegmentReplicantLookup
{
  public static SegmentReplicantLookup make(DruidCluster cluster)
  {
    final Table<SegmentId, String, ReplicaCount> replicaCounts = HashBasedTable.create();
    cluster.getHistoricals().forEach(
        (tier, historicals) -> historicals.forEach(
            serverHolder -> {
              // Add segments already loaded on this server
              for (DataSegment segment : serverHolder.getServer().iterateAllSegments()) {
                computeIfAbsent(replicaCounts, segment.getId(), tier).addLoaded();
              }

              // Add segments queued for load, drop or move on this server
              serverHolder.getQueuedSegments().forEach(
                  (segment, state) ->
                      computeIfAbsent(replicaCounts, segment.getId(), tier).addQueued(state)
              );
            }
        )
    );

    cluster.getBrokers().forEach(broker -> {
      final ImmutableDruidServer server = broker.getServer();
      for (DataSegment segment : server.iterateAllSegments()) {
        computeIfAbsent(replicaCounts, segment.getId(), server.getTier())
            .addLoadedBroadcast();
      }
    });

    cluster.getRealtimes().forEach(realtime -> {
      final ImmutableDruidServer server = realtime.getServer();
      for (DataSegment segment : server.iterateAllSegments()) {
        computeIfAbsent(replicaCounts, segment.getId(), server.getTier())
            .addLoadedBroadcast();
      }
    });

    return new SegmentReplicantLookup(replicaCounts, cluster);
  }

  private static ReplicaCount computeIfAbsent(
      Table<SegmentId, String, ReplicaCount> replicaCounts,
      SegmentId segmentId,
      String tier
  )
  {
    ReplicaCount count = replicaCounts.get(segmentId, tier);
    if (count == null) {
      count = new ReplicaCount();
      replicaCounts.put(segmentId, tier, count);
    }
    return count;
  }

  private final Table<SegmentId, String, ReplicaCount> replicaCounts;
  private final Map<String, Integer> tierToHistoricalCount = new HashMap<>();

  private SegmentReplicantLookup(
      Table<SegmentId, String, ReplicaCount> replicaCounts,
      DruidCluster cluster
  )
  {
    this.replicaCounts = replicaCounts;

    cluster.getHistoricals().forEach(
        (tier, historicals) -> tierToHistoricalCount.put(tier, historicals.size())
    );
  }

  /**
   * Number of replicas of the segment currently being moved in the given tier.
   */
  public int getMovingReplicas(SegmentId segmentId, String tier)
  {
    ReplicaCount count = replicaCounts.get(segmentId, tier);
    return (count == null) ? 0 : count.moving;
  }

  /**
   * Number of replicas of the segment which are safely loaded on the given tier
   * and are not being dropped.
   */
  public int getLoadedNotDroppingReplicas(SegmentId segmentId, String tier)
  {
    ReplicaCount count = replicaCounts.get(segmentId, tier);
    return (count == null) ? 0 : count.loadedNotDropping();
  }

  public int getLoadingReplicas(SegmentId segmentId, String tier)
  {
    ReplicaCount count = replicaCounts.get(segmentId, tier);
    return count == null ? 0 : count.loading;
  }

  /**
   * Number of replicas of the segment which are loaded on the cluster.
   *
   * @param includeDropping Whether segments which are being dropped should be
   *                        included in the total count.
   */
  public int getLoadedReplicas(SegmentId segmentId, boolean includeDropping)
  {
    final Map<String, ReplicaCount> allTiers = replicaCounts.row(segmentId);
    int totalLoaded = 0;
    for (ReplicaCount count : allTiers.values()) {
      totalLoaded += includeDropping ? count.loaded : count.loadedNotDropping();
    }
    return totalLoaded;
  }

  /**
   * Sets the number of replicas required for the specified segment in the tier.
   * In a given coordinator run, this method must be called atleast once for
   * every segment every tier.
   */
  public void setRequiredReplicas(SegmentId segmentId, boolean isBroadcast, String tier, int requiredReplicas)
  {
    ReplicaCount counts = computeIfAbsent(replicaCounts, segmentId, tier);
    counts.required = requiredReplicas;
    if (isBroadcast) {
      counts.possible = requiredReplicas;
    } else {
      counts.possible = tierToHistoricalCount.getOrDefault(tier, 0);
    }
  }

  public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicated(
      Iterable<DataSegment> usedSegments,
      boolean ignoreMissingServers
  )
  {
    final Map<String, Object2LongMap<String>> tierToUnderReplicated = new HashMap<>();

    for (DataSegment segment : usedSegments) {
      final Map<String, ReplicaCount> tierToReplicaCount = replicaCounts.row(segment.getId());
      if (tierToReplicaCount == null) {
        continue;
      }

      tierToReplicaCount.forEach((tier, counts) -> {
        final int underReplicated = counts.underReplicated(ignoreMissingServers);
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
   * Counts of replicas of a segment in different states.
   */
  private static class ReplicaCount
  {
    int possible;
    int required;
    int loaded;
    int loadedBroadcast;
    int loading;
    int dropping;
    int moving;

    void addLoaded()
    {
      ++loaded;
    }

    /**
     * Increments number of segments loaded on non-historical servers. This value
     * is used only for computing level of under-replication of broadcast segments.
     */
    void addLoadedBroadcast()
    {
      ++loadedBroadcast;
    }

    void addQueued(SegmentAction action)
    {
      switch (action) {
        case REPLICATE:
        case LOAD:
          ++loading;
          break;
        case MOVE_TO:
          ++moving;
          break;
        case DROP:
          ++dropping;
          break;
        default:
          break;
      }
    }

    int loadedNotDropping()
    {
      return loaded - dropping;
    }

    int underReplicated(boolean ignoreMissingServers)
    {
      int totalLoaded = loaded + loadedBroadcast;
      int targetCount = ignoreMissingServers ? required : Math.min(required, possible);
      return targetCount > totalLoaded ? targetCount - totalLoaded : 0;
    }
  }
}
