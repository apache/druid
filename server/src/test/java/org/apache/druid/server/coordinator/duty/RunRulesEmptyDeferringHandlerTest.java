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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.server.coordinator.rules.PartialLoadMatcher;
import org.apache.druid.server.coordinator.rules.SegmentActionHandler;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RunRulesEmptyDeferringHandlerTest
{
  private static final Map<String, Integer> TIER1_REPLICANTS = Map.of("tier1", 1);
  private static final String POSITIVE_FINGERPRINT = "v1:positive";

  @Test
  void positiveLoadPassesThroughImmediately()
  {
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);
    final DataSegment seg = segment(0);
    final PartialLoadProfile positive = profile(POSITIVE_FINGERPRINT);

    handler.replicateSegmentPartially(seg, positive, TIER1_REPLICANTS);

    // Positive load is dispatched immediately, not buffered.
    Assertions.assertEquals(1, delegate.partialLoads.size());
    Assertions.assertEquals(seg, delegate.partialLoads.get(0).segment);
    Assertions.assertSame(positive, delegate.partialLoads.get(0).profile);
  }

  @Test
  void emptyLoadIsBufferedUntilFlush()
  {
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);
    final DataSegment seg = segment(0);
    final PartialLoadProfile empty = profile(PartialLoadMatcher.EMPTY_LOAD_FINGERPRINT);

    handler.replicateSegmentPartially(seg, empty, TIER1_REPLICANTS);

    // Empty load is buffered, not dispatched.
    Assertions.assertTrue(delegate.partialLoads.isEmpty());
  }

  @Test
  void flushDispatchesBufferedEmptiesWhenPositiveSeen()
  {
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);
    final DataSegment p0 = segment(0);
    final DataSegment p1 = segment(1);
    final PartialLoadProfile positive = profile(POSITIVE_FINGERPRINT);
    final PartialLoadProfile empty = profile(PartialLoadMatcher.EMPTY_LOAD_FINGERPRINT);

    handler.replicateSegmentPartially(p0, positive, TIER1_REPLICANTS);
    handler.replicateSegmentPartially(p1, empty, TIER1_REPLICANTS);
    // After the group: p0 dispatched (positive), p1 buffered.
    Assertions.assertEquals(1, delegate.partialLoads.size());

    handler.flushAndReset();
    // Now p1 should be dispatched too — the group had a positive match.
    Assertions.assertEquals(2, delegate.partialLoads.size());
    Assertions.assertEquals(p1, delegate.partialLoads.get(1).segment);
  }

  @Test
  void flushDiscardsBufferedEmptiesWhenNoPositive()
  {
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);
    final PartialLoadProfile empty = profile(PartialLoadMatcher.EMPTY_LOAD_FINGERPRINT);

    handler.replicateSegmentPartially(segment(0), empty, TIER1_REPLICANTS);
    handler.replicateSegmentPartially(segment(1), empty, TIER1_REPLICANTS);

    handler.flushAndReset();

    // No positive in the group — buffered empties are discarded, never dispatched.
    Assertions.assertTrue(delegate.partialLoads.isEmpty());
  }

  @Test
  void flushResetsStateBetweenGroups()
  {
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);
    final PartialLoadProfile positive = profile(POSITIVE_FINGERPRINT);
    final PartialLoadProfile empty = profile(PartialLoadMatcher.EMPTY_LOAD_FINGERPRINT);

    // Group 1: has a positive match → buffered empty gets dispatched on flush.
    handler.replicateSegmentPartially(segment(0), positive, TIER1_REPLICANTS);
    handler.replicateSegmentPartially(segment(1), empty, TIER1_REPLICANTS);
    handler.flushAndReset();
    Assertions.assertEquals(2, delegate.partialLoads.size());

    // Group 2: only empties → discarded on flush. The previous group's positive must not leak through.
    handler.replicateSegmentPartially(segment(10), empty, TIER1_REPLICANTS);
    handler.replicateSegmentPartially(segment(11), empty, TIER1_REPLICANTS);
    handler.flushAndReset();
    Assertions.assertEquals(2, delegate.partialLoads.size()); // unchanged
  }

  @Test
  void flushOnEmptyBufferIsNoOp()
  {
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);

    handler.flushAndReset();

    Assertions.assertTrue(delegate.partialLoads.isEmpty());
  }

  @Test
  void fullLoadPassesThroughUnchanged()
  {
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);
    final DataSegment seg = segment(0);

    handler.replicateSegment(seg, TIER1_REPLICANTS);

    Assertions.assertEquals(1, delegate.fullLoads.size());
    Assertions.assertEquals(seg, delegate.fullLoads.get(0).segment);
  }

  @Test
  void broadcastPassesThroughUnchanged()
  {
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);
    final DataSegment seg = segment(0);

    handler.broadcastSegment(seg);

    Assertions.assertEquals(List.of(seg), delegate.broadcasts);
  }

  @Test
  void deletePassesThroughUnchanged()
  {
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);
    final DataSegment seg = segment(0);

    handler.deleteSegment(seg);

    Assertions.assertEquals(List.of(seg), delegate.deletes);
  }

  @Test
  void positiveLoadDispatchedAfterBufferedEmptyStillFlushesEmpty()
  {
    // Order shouldn't matter: empties buffered before the positive arrives should still get flushed because the
    // positive marks the group as having a positive match.
    final RecordingHandler delegate = new RecordingHandler();
    final RunRules.EmptyDeferringHandler handler = new RunRules.EmptyDeferringHandler(delegate);
    final PartialLoadProfile positive = profile(POSITIVE_FINGERPRINT);
    final PartialLoadProfile empty = profile(PartialLoadMatcher.EMPTY_LOAD_FINGERPRINT);

    handler.replicateSegmentPartially(segment(0), empty, TIER1_REPLICANTS);
    handler.replicateSegmentPartially(segment(1), positive, TIER1_REPLICANTS);
    Assertions.assertEquals(1, delegate.partialLoads.size()); // positive only

    handler.flushAndReset();
    Assertions.assertEquals(2, delegate.partialLoads.size()); // empty now flushed too
  }

  private static DataSegment segment(int partitionNum)
  {
    final NumberedShardSpec shardSpec = new NumberedShardSpec(partitionNum, 2);
    return DataSegment
        .builder(SegmentId.of("ds", Intervals.of("2026-01-01/2026-01-02"), "v", shardSpec))
        .shardSpec(shardSpec)
        .loadSpec(Map.of("type", "local", "path", "/seg"))
        .size(0)
        .build();
  }

  private static PartialLoadProfile profile(String fingerprint)
  {
    return PartialLoadProfile.forRequest(Map.of("type", "test", "fingerprint", fingerprint), fingerprint);
  }

  private record RecordedPartialLoad(DataSegment segment, PartialLoadProfile profile, Map<String, Integer> replicants)
  {
  }

  private record RecordedFullLoad(DataSegment segment, Map<String, Integer> replicants)
  {
  }

  private static final class RecordingHandler implements SegmentActionHandler
  {
    final List<RecordedPartialLoad> partialLoads = new ArrayList<>();
    final List<RecordedFullLoad> fullLoads = new ArrayList<>();
    final List<DataSegment> broadcasts = new ArrayList<>();
    final List<DataSegment> deletes = new ArrayList<>();

    @Override
    public void replicateSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount)
    {
      fullLoads.add(new RecordedFullLoad(segment, new HashMap<>(tierToReplicaCount)));
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
      broadcasts.add(segment);
    }

    @Override
    public void deleteSegment(DataSegment segment)
    {
      deletes.add(segment);
    }
  }
}
