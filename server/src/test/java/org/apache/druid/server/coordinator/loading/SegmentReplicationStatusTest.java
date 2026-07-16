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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies that {@link SegmentReplicationStatus#computeSegmentStats} produces results
 * identical to computing unavailable, under-replicated and deep-storage-only counts
 * independently via {@link SegmentReplicationStatus#getReplicaCountsInCluster} and
 * {@link SegmentReplicationStatus#getTierToDatasourceToUnderReplicated}.
 */
public class SegmentReplicationStatusTest
{
  private static final String TIER_1 = "tier1";
  private static final String TIER_2 = "tier2";

  private final List<DataSegment> segments = CreateDataSegments
      .ofDatasource("wiki")
      .forIntervals(5, Granularities.DAY)
      .eachOfSize(100);

  @Test
  public void testComputeSegmentStatsMatchesIndependentComputation()
  {
    final Map<SegmentId, Map<String, SegmentReplicaCount>> replicaCountsInTier = new HashMap<>();

    // Segment 0: fully loaded, single tier, no under-replication.
    replicaCountsInTier.put(segments.get(0).getId(), Map.of(TIER_1, countOf(2, 2, 2)));

    // Segment 1: unavailable (required > 0, nothing loaded).
    replicaCountsInTier.put(segments.get(1).getId(), Map.of(TIER_1, countOf(2, 2, 0)));

    // Segment 2: deep-storage-only (required == 0, nothing loaded).
    replicaCountsInTier.put(segments.get(2).getId(), Map.of(TIER_1, countOf(0, 0, 0)));

    // Segment 3: under-replicated in one tier, satisfied in another.
    final Map<String, SegmentReplicaCount> tierMap3 = new HashMap<>();
    tierMap3.put(TIER_1, countOf(2, 2, 1));
    tierMap3.put(TIER_2, countOf(1, 1, 1));
    replicaCountsInTier.put(segments.get(3).getId(), tierMap3);

    // Segment 4: present in the used-segment list but absent from the replication map
    // (e.g. metadata race) -- must be treated as unavailable, not throw.
    // Intentionally not added to replicaCountsInTier.

    final SegmentReplicationStatus status = new SegmentReplicationStatus(replicaCountsInTier);

    for (boolean ignoreMissingServers : new boolean[]{true, false}) {
      final SegmentStatsSnapshot snapshot =
          status.computeSegmentStats(segments, ignoreMissingServers);

      final Object2IntMap<String> expectedUnavailable = computeExpectedUnavailable(status, segments);
      final Object2IntMap<String> expectedDeepStorageOnly = computeExpectedDeepStorageOnly(status, segments);
      final Map<String, Object2LongMap<String>> expectedUnderReplicated =
          status.getTierToDatasourceToUnderReplicated(segments, ignoreMissingServers);

      Assert.assertEquals(expectedUnavailable, snapshot.getDatasourceToUnavailableCount());
      Assert.assertEquals(expectedDeepStorageOnly, snapshot.getDatasourceToDeepStorageOnlyCount());
      Assert.assertEquals(expectedUnderReplicated, snapshot.getTierToDatasourceToUnderReplicatedCount());
    }
  }

  @Test
  public void testIgnoreMissingServersUsesMissingNotMissingAndLoadable()
  {
    final Map<SegmentId, Map<String, SegmentReplicaCount>> replicaCountsInTier = new HashMap<>();

    // required=3, requiredAndLoadable=1 (only 1 loadable server), loaded=0.
    // missing() = 3, missingAndLoadable() = 1.
    final SegmentReplicaCount count = new SegmentReplicaCount();
    count.setRequired(3, 1);
    replicaCountsInTier.put(segments.get(0).getId(), Map.of(TIER_1, count));

    final SegmentReplicationStatus status = new SegmentReplicationStatus(replicaCountsInTier);
    final List<DataSegment> singleSegment = List.of(segments.get(0));

    final SegmentStatsSnapshot ignoreMissing =
        status.computeSegmentStats(singleSegment, true);
    Assert.assertEquals(
        3L,
        ignoreMissing.getTierToDatasourceToUnderReplicatedCount().get(TIER_1).getLong("wiki")
    );

    final SegmentStatsSnapshot respectMissing =
        status.computeSegmentStats(singleSegment, false);
    Assert.assertEquals(
        1L,
        respectMissing.getTierToDatasourceToUnderReplicatedCount().get(TIER_1).getLong("wiki")
    );
  }

  @Test
  public void testConstructorSnapshotsInputAndIsUnaffectedByLaterMutation()
  {
    final Map<SegmentId, Map<String, SegmentReplicaCount>> replicaCountsInTier = new HashMap<>();
    final SegmentReplicaCount count = countOf(2, 2, 1);
    final Map<String, SegmentReplicaCount> tierMap = new HashMap<>();
    tierMap.put(TIER_1, count);
    replicaCountsInTier.put(segments.get(0).getId(), tierMap);

    final SegmentReplicationStatus status = new SegmentReplicationStatus(replicaCountsInTier);
    final SegmentReplicaCount totalBefore = status.getReplicaCountsInCluster(segments.get(0).getId());
    Assert.assertEquals(1, totalBefore.totalLoaded());

    // Mutate the caller's inputs after construction, as BalanceSegments does later in the same
    // coordinator cycle: add a brand new segment key and mutate an already-tracked count in place.
    count.incrementLoaded();
    tierMap.put(TIER_2, countOf(1, 1, 1));
    replicaCountsInTier.put(segments.get(1).getId(), Map.of(TIER_1, countOf(1, 1, 1)));

    Assert.assertEquals(1, status.getReplicaCountsInCluster(segments.get(0).getId()).totalLoaded());
    Assert.assertNull(status.getReplicaCountsInCluster(segments.get(1).getId()));
  }

  private static SegmentReplicaCount countOf(int required, int requiredAndLoadable, int loaded)
  {
    final SegmentReplicaCount count = new SegmentReplicaCount();
    count.setRequired(required, requiredAndLoadable);
    for (int i = 0; i < loaded; i++) {
      count.incrementLoaded();
    }
    return count;
  }

  private static Object2IntMap<String> computeExpectedUnavailable(
      SegmentReplicationStatus status,
      List<DataSegment> segments
  )
  {
    final it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap<String> result =
        new it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap<>();
    for (DataSegment segment : segments) {
      final SegmentReplicaCount rc = status.getReplicaCountsInCluster(segment.getId());
      if (rc != null && (rc.totalLoaded() > 0 || rc.required() == 0)) {
        result.addTo(segment.getDataSource(), 0);
      } else {
        result.addTo(segment.getDataSource(), 1);
      }
    }
    return result;
  }

  private static Object2IntMap<String> computeExpectedDeepStorageOnly(
      SegmentReplicationStatus status,
      List<DataSegment> segments
  )
  {
    final it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap<String> result =
        new it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap<>();
    for (DataSegment segment : segments) {
      final SegmentReplicaCount rc = status.getReplicaCountsInCluster(segment.getId());
      if (rc != null && rc.totalLoaded() == 0 && rc.required() == 0) {
        result.addTo(segment.getDataSource(), 1);
      }
    }
    return result;
  }
}
