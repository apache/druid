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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.server.coordinator.rules.PartialLoadMatcher;
import org.apache.druid.server.coordinator.rules.SegmentActionHandler;
import org.apache.druid.server.coordinator.rules.ShardGroupFollowup;
import org.apache.druid.server.coordinator.rules.WildcardClusterGroupPartialLoadMatcher;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class RunRulesShardGroupFollowupTest
{
  private static final String DATASOURCE = "wiki";
  private static final Interval INTERVAL = Intervals.of("2024-01-01/2024-01-02");
  private static final String VERSION = "v1";
  private static final Map<String, Integer> TIER1_REPLICANTS = Map.of("tier1", 1);

  @Test
  void emptyFollowupsIsNoOp()
  {
    final RecordingHandler handler = new RecordingHandler();
    RunRules.flushShardGroupFollowups(
        List.of(),
        null,
        null,
        null,
        DataSourcesSnapshot.fromUsedSegments(List.of()),
        handler
    );
    Assertions.assertTrue(handler.partialLoads.isEmpty());
  }

  @Test
  void asymmetricMatcherDispatchesEmptyLoadToUnmatchedSibling()
  {
    // 2-partition shard group: partition-0 contains cluster-A, partition-1 contains cluster-B.
    // The rule's matcher targets cluster-A, so partition-0 was matched in the per-segment pass and partition-1 was
    // not. The flush must dispatch an emptyMatch load to partition-1 so the shard group is uniformly placed.
    final DataSegment p0 = clusteredSegment(0, "cluster-A");
    final DataSegment p1 = clusteredSegment(1, "cluster-B");
    final PartialLoadMatcher matcher = clusterMatcher("cluster-A");

    final DataSourcesSnapshot snapshot = DataSourcesSnapshot.fromUsedSegments(List.of(p0, p1));
    final RecordingHandler handler = new RecordingHandler();
    final List<ShardGroupFollowup> followups = List.of(new ShardGroupFollowup(p0, matcher, TIER1_REPLICANTS));

    RunRules.flushShardGroupFollowups(followups, DATASOURCE, INTERVAL, VERSION, snapshot, handler);

    Assertions.assertEquals(1, handler.partialLoads.size());
    final RecordedPartialLoad emptyLoad = handler.partialLoads.get(0);
    Assertions.assertEquals(p1, emptyLoad.segment);
    Assertions.assertEquals(List.of(), emptyLoad.profile.wrappedLoadSpec().get("clusterGroupIndices"));
    Assertions.assertEquals(TIER1_REPLICANTS, emptyLoad.tieredReplicants);
  }

  @Test
  void matchedSiblingIsNotEmptyLoadedAgain()
  {
    // Two segments both positively matched in the per-segment pass. The flush should not re-load either of them —
    // both are already covered by their original positive loads.
    final DataSegment p0 = clusteredSegment(0, "cluster-A");
    final DataSegment p1 = clusteredSegment(1, "cluster-A");
    final PartialLoadMatcher matcher = clusterMatcher("cluster-A");

    final DataSourcesSnapshot snapshot = DataSourcesSnapshot.fromUsedSegments(List.of(p0, p1));
    final RecordingHandler handler = new RecordingHandler();
    final List<ShardGroupFollowup> followups = List.of(
        new ShardGroupFollowup(p0, matcher, TIER1_REPLICANTS),
        new ShardGroupFollowup(p1, matcher, TIER1_REPLICANTS)
    );

    RunRules.flushShardGroupFollowups(followups, DATASOURCE, INTERVAL, VERSION, snapshot, handler);

    Assertions.assertTrue(handler.partialLoads.isEmpty());
  }

  @Test
  void matcherWithoutEmptyMatchSkipsFlush()
  {
    // A matcher that returns null from emptyMatch (the default) opts out of the post-pass. Even though there are
    // unmatched siblings in the group, no empty-loads are dispatched — symmetric matchers don't need this.
    final DataSegment p0 = clusteredSegment(0, "cluster-A");
    final DataSegment p1 = clusteredSegment(1, "cluster-B");
    final PartialLoadMatcher noEmptyMatcher = new PartialLoadMatcher()
    {
      @Override
      public MatchResult match(DataSegment segment, Map<String, Object> baseLoadSpec)
      {
        return null;
      }
    };

    final DataSourcesSnapshot snapshot = DataSourcesSnapshot.fromUsedSegments(List.of(p0, p1));
    final RecordingHandler handler = new RecordingHandler();
    final List<ShardGroupFollowup> followups =
        List.of(new ShardGroupFollowup(p0, noEmptyMatcher, TIER1_REPLICANTS));

    RunRules.flushShardGroupFollowups(followups, DATASOURCE, INTERVAL, VERSION, snapshot, handler);

    Assertions.assertTrue(handler.partialLoads.isEmpty());
  }

  @Test
  void flushOnlyTouchesSiblingsInTheSpecifiedVersion()
  {
    // Two versions of the same interval, each a distinct shard group. Flushing for v1 must not look at v2's
    // partition, even though both are in the snapshot.
    final DataSegment v1p0 = clusteredSegmentWithVersion(0, "cluster-A", "v1");
    final DataSegment v2p0 = clusteredSegmentWithVersion(0, "cluster-A", "v2");
    final PartialLoadMatcher matcher = clusterMatcher("cluster-A");

    final DataSourcesSnapshot snapshot = DataSourcesSnapshot.fromUsedSegments(List.of(v1p0, v2p0));
    final RecordingHandler handler = new RecordingHandler();
    final List<ShardGroupFollowup> followups = List.of(new ShardGroupFollowup(v1p0, matcher, TIER1_REPLICANTS));

    RunRules.flushShardGroupFollowups(followups, DATASOURCE, INTERVAL, "v1", snapshot, handler);

    // v1p0 was matched; v2p0 belongs to a different shard group and must not be touched.
    Assertions.assertTrue(handler.partialLoads.isEmpty());
  }

  @Test
  void multipleFollowupsInSameGroupAreDeduped()
  {
    // 3-partition shard group: matcher targets cluster-A and matched partition-0 and partition-1. Two followups
    // in the same group. The flush dispatches exactly one empty-load (for partition-2), not two — the holder is
    // walked once per matcher, not once per followup.
    final DataSegment p0 = clusteredSegment(0, "cluster-A");
    final DataSegment p1 = clusteredSegment(1, "cluster-A");
    final DataSegment p2 = clusteredSegment(2, "cluster-B");
    final PartialLoadMatcher matcher = clusterMatcher("cluster-A");

    final DataSourcesSnapshot snapshot = DataSourcesSnapshot.fromUsedSegments(List.of(p0, p1, p2));
    final RecordingHandler handler = new RecordingHandler();
    final List<ShardGroupFollowup> followups = List.of(
        new ShardGroupFollowup(p0, matcher, TIER1_REPLICANTS),
        new ShardGroupFollowup(p1, matcher, TIER1_REPLICANTS)
    );

    RunRules.flushShardGroupFollowups(followups, DATASOURCE, INTERVAL, VERSION, snapshot, handler);

    Assertions.assertEquals(1, handler.partialLoads.size());
    Assertions.assertEquals(p2, handler.partialLoads.get(0).segment);
  }

  @Test
  void differentMatchersInSameGroupAreProcessedIndependently()
  {
    // Same shard group, two distinct matcher instances. The flush groups by matcher reference identity and
    // dispatches each matcher's empty-loads to its own unmatched siblings.
    final DataSegment p0 = clusteredSegment(0, "cluster-A");
    final DataSegment p1 = clusteredSegment(1, "cluster-B");
    final PartialLoadMatcher matcherA = clusterMatcher("cluster-A");
    final PartialLoadMatcher matcherB = clusterMatcher("cluster-B");

    final DataSourcesSnapshot snapshot = DataSourcesSnapshot.fromUsedSegments(List.of(p0, p1));
    final RecordingHandler handler = new RecordingHandler();
    final List<ShardGroupFollowup> followups = List.of(
        new ShardGroupFollowup(p0, matcherA, TIER1_REPLICANTS),
        new ShardGroupFollowup(p1, matcherB, TIER1_REPLICANTS)
    );

    RunRules.flushShardGroupFollowups(followups, DATASOURCE, INTERVAL, VERSION, snapshot, handler);

    // matcherA matched p0, didn't match p1 → empty-load on p1.
    // matcherB matched p1, didn't match p0 → empty-load on p0.
    Assertions.assertEquals(2, handler.partialLoads.size());
    final Set<DataSegment> emptyLoaded = new HashSet<>();
    handler.partialLoads.forEach(load -> emptyLoaded.add(load.segment));
    Assertions.assertEquals(Set.of(p0, p1), emptyLoaded);
  }

  @Test
  void appendedSiblingIsNotEmptyLoaded()
  {
    // 2-core-partition shard group plus one appended sibling (partitionNum=2 with numCorePartitions=2). The
    // matcher matches core p0 but not core p1 and not the appended segment. The flush must empty-load core p1
    // (completeness-required) but skip the appended sibling — appended segments are queried individually by the
    // broker and don't participate in the core-group completeness check.
    final DataSegment corePartition0 = numberedSegment(0, 2, "cluster-A");
    final DataSegment corePartition1 = numberedSegment(1, 2, "cluster-B");
    final DataSegment appendedPartition = numberedSegment(2, 2, "cluster-C");
    final PartialLoadMatcher matcher = clusterMatcher("cluster-A");

    final DataSourcesSnapshot snapshot =
        DataSourcesSnapshot.fromUsedSegments(List.of(corePartition0, corePartition1, appendedPartition));
    final RecordingHandler handler = new RecordingHandler();
    final List<ShardGroupFollowup> followups =
        List.of(new ShardGroupFollowup(corePartition0, matcher, TIER1_REPLICANTS));

    RunRules.flushShardGroupFollowups(followups, DATASOURCE, INTERVAL, VERSION, snapshot, handler);

    Assertions.assertEquals(1, handler.partialLoads.size());
    Assertions.assertEquals(corePartition1, handler.partialLoads.get(0).segment);
  }

  @Test
  void datasourceMissingFromSnapshotIsSkipped()
  {
    // A followup whose datasource isn't in the snapshot (segment was dropped between collecting followups and
    // flushing) should be silently skipped, not throw.
    final DataSegment p0 = clusteredSegment(0, "cluster-A");
    final PartialLoadMatcher matcher = clusterMatcher("cluster-A");

    final DataSourcesSnapshot snapshot = DataSourcesSnapshot.fromUsedSegments(List.of());
    final RecordingHandler handler = new RecordingHandler();
    final List<ShardGroupFollowup> followups = List.of(new ShardGroupFollowup(p0, matcher, TIER1_REPLICANTS));

    RunRules.flushShardGroupFollowups(followups, DATASOURCE, INTERVAL, VERSION, snapshot, handler);

    Assertions.assertTrue(handler.partialLoads.isEmpty());
  }

  private static DataSegment clusteredSegment(int partitionNum, String cluster)
  {
    return clusteredSegmentWithVersion(partitionNum, cluster, VERSION);
  }

  private static DataSegment clusteredSegmentWithVersion(int partitionNum, String cluster, String version)
  {
    return numberedSegmentWithVersion(partitionNum, 3, cluster, version);
  }

  private static DataSegment numberedSegment(int partitionNum, int numCorePartitions, String cluster)
  {
    return numberedSegmentWithVersion(partitionNum, numCorePartitions, cluster, VERSION);
  }

  /**
   * Builds a clustered shard-group sibling with explicit core-partition count.
   * {@link DataSegment#builder(SegmentId)} does not propagate the shardSpec from the SegmentId, so we set it
   * explicitly so partitions get distinct segment IDs.
   */
  private static DataSegment numberedSegmentWithVersion(
      int partitionNum,
      int numCorePartitions,
      String cluster,
      String version
  )
  {
    final NumberedShardSpec shardSpec = new NumberedShardSpec(partitionNum, numCorePartitions);
    return DataSegment
        .builder(SegmentId.of(DATASOURCE, INTERVAL, version, shardSpec))
        .shardSpec(shardSpec)
        .loadSpec(Map.of("type", "local", "path", "/var/druid/segments/foo"))
        .clusterGroups(new ClusterGroupTuples(
            RowSignature.builder().add("name", ColumnType.STRING).build(),
            List.of(List.of(cluster))
        ))
        .size(0)
        .build();
  }

  private static PartialLoadMatcher clusterMatcher(String name)
  {
    return new WildcardClusterGroupPartialLoadMatcher(List.of(Map.of("name", name)), null);
  }

  private record RecordedPartialLoad(
      DataSegment segment,
      PartialLoadProfile profile,
      Map<String, Integer> tieredReplicants
  )
  {
  }

  private static final class RecordingHandler implements SegmentActionHandler
  {
    final List<RecordedPartialLoad> partialLoads = new ArrayList<>();

    @Override
    public void replicateSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount)
    {
      throw new AssertionError("Flush must only call replicateSegmentPartially, not replicateSegment");
    }

    @Override
    public void replicateSegmentPartially(
        DataSegment segment,
        PartialLoadProfile profile,
        Map<String, Integer> tierToReplicaCount
    )
    {
      partialLoads.add(new RecordedPartialLoad(segment, profile, new HashMap<>(tierToReplicaCount)));
    }

    @Override
    public void broadcastSegment(DataSegment segment)
    {
      throw new AssertionError("Flush must not call broadcastSegment");
    }

    @Override
    public void deleteSegment(DataSegment segment)
    {
      throw new AssertionError("Flush must not call deleteSegment");
    }
  }
}
