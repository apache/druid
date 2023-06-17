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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.server.coordinator.DruidCluster;
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
    final Table<SegmentId, String, SegmentReplicaCount> replicaCounts = HashBasedTable.create();

    cluster.getHistoricals().forEach(
        (tier, historicals) -> historicals.forEach(
            serverHolder -> {
              // Add segments already loaded on this server
              for (DataSegment segment : serverHolder.getServer().iterateAllSegments()) {
                computeIfAbsent(replicaCounts, segment.getId(), tier)
                    .addLoaded();
              }

              // Add segments queued for load, drop or move on this server
              serverHolder.getQueuedSegments().forEach(
                  (segment, state) ->
                      computeIfAbsent(replicaCounts, segment.getId(), tier)
                          .addQueued(state)
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

  private static SegmentReplicaCount computeIfAbsent(
      Table<SegmentId, String, SegmentReplicaCount> replicaCounts,
      SegmentId segmentId,
      String tier
  )
  {
    SegmentReplicaCount count = replicaCounts.get(segmentId, tier);
    if (count == null) {
      count = new SegmentReplicaCount();
      replicaCounts.put(segmentId, tier, count);
    }
    return count;
  }

  private final Table<SegmentId, String, SegmentReplicaCount> replicaCounts;
  private final Map<String, Integer> tierToHistoricalCount = new HashMap<>();

  private SegmentReplicantLookup(
      Table<SegmentId, String, SegmentReplicaCount> replicaCounts,
      DruidCluster cluster
  )
  {
    this.replicaCounts = replicaCounts;

    cluster.getHistoricals().forEach(
        (tier, historicals) -> tierToHistoricalCount.put(tier, historicals.size())
    );
  }

  public SegmentReplicaCount getReplicaCountsOnTier(SegmentId segmentId, String tier)
  {
    SegmentReplicaCount count = replicaCounts.get(segmentId, tier);
    return count == null ? SegmentReplicaCount.empty() : count;
  }

  public SegmentReplicaCount getReplicaCountsInCluster(SegmentId segmentId)
  {
    final SegmentReplicaCount total = new SegmentReplicaCount();
    replicaCounts.row(segmentId).values()
                 .forEach(total::accumulate);
    return total;
  }

  /**
   * Sets the number of replicas required for the specified segment in the tier.
   * In a given coordinator run, this method must be called at least once for
   * every used segment for every tier.
   */
  public void setRequiredReplicas(SegmentId segmentId, String tier, int requiredReplicas)
  {
    SegmentReplicaCount counts = computeIfAbsent(replicaCounts, segmentId, tier);
    counts.setRequired(requiredReplicas);
    counts.setPossible(tierToHistoricalCount.getOrDefault(tier, 0));
  }

  public void setRequiredBroadcastReplicas(SegmentId segmentId, String tier, int requiredReplicas)
  {
    SegmentReplicaCount counts = computeIfAbsent(replicaCounts, segmentId, tier);
    counts.setRequired(requiredReplicas);
    counts.setPossible(requiredReplicas);
  }

  public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicated(
      Iterable<DataSegment> usedSegments,
      boolean ignoreMissingServers
  )
  {
    final Map<String, Object2LongMap<String>> tierToUnderReplicated = new HashMap<>();

    for (DataSegment segment : usedSegments) {
      final Map<String, SegmentReplicaCount> tierToReplicaCount = replicaCounts.row(segment.getId());
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

}
