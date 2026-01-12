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

package org.apache.druid.segment.metadata;

import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.timeline.CompactionState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompactionStateCacheTest
{
  private CompactionStateCache cache;

  @BeforeEach
  public void setUp()
  {
    cache = new CompactionStateCache();
  }

  @Test
  public void test_isEnabled_returnsTrue()
  {
    assertTrue(cache.isEnabled());
  }

  @Test
  public void test_getCompactionStateByFingerprint_emptyCache_returnsEmpty()
  {
    Optional<CompactionState> result = cache.getCompactionStateByFingerprint("nonexistent");
    assertFalse(result.isPresent());
  }

  @Test
  public void test_getCompactionStateByFingerprint_nullFingerprint_returnsEmpty()
  {
    Optional<CompactionState> result = cache.getCompactionStateByFingerprint(null);
    assertFalse(result.isPresent());
  }

  @Test
  public void test_resetCompactionStatesForPublishedSegments_andThen_getCompactionStateByFingerprint()
  {
    CompactionState state1 = createTestCompactionState();
    CompactionState state2 = createTestCompactionState();

    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);
    stateMap.put("fingerprint2", state2);

    cache.resetCompactionStatesForPublishedSegments(stateMap);

    Optional<CompactionState> result1 = cache.getCompactionStateByFingerprint("fingerprint1");
    assertTrue(result1.isPresent());
    assertEquals(state1, result1.get());

    Optional<CompactionState> result2 = cache.getCompactionStateByFingerprint("fingerprint2");
    assertTrue(result2.isPresent());
    assertEquals(state2, result2.get());

    Optional<CompactionState> result3 = cache.getCompactionStateByFingerprint("nonexistent");
    assertFalse(result3.isPresent());
  }

  @Test
  public void test_getPublishedCompactionStateMap_returnsImmutableSnapshot()
  {
    CompactionState state1 = createTestCompactionState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetCompactionStatesForPublishedSegments(stateMap);

    Map<String, CompactionState> retrieved = cache.getPublishedCompactionStateMap();
    assertEquals(1, retrieved.size());
    assertEquals(state1, retrieved.get("fingerprint1"));
  }

  @Test
  public void test_clear_emptiesCache()
  {
    CompactionState state1 = createTestCompactionState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetCompactionStatesForPublishedSegments(stateMap);

    Optional<CompactionState> beforeClear = cache.getCompactionStateByFingerprint("fingerprint1");
    assertTrue(beforeClear.isPresent());

    cache.clear();

    Optional<CompactionState> afterClear = cache.getCompactionStateByFingerprint("fingerprint1");
    assertFalse(afterClear.isPresent());

    Map<String, CompactionState> mapAfterClear = cache.getPublishedCompactionStateMap();
    assertEquals(0, mapAfterClear.size());
  }

  @Test
  public void test_stats_trackHitsAndMisses()
  {
    CompactionState state1 = createTestCompactionState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetCompactionStatesForPublishedSegments(stateMap);

    // Generate 3 hits
    cache.getCompactionStateByFingerprint("fingerprint1");
    cache.getCompactionStateByFingerprint("fingerprint1");
    cache.getCompactionStateByFingerprint("fingerprint1");

    // Generate 2 misses
    cache.getCompactionStateByFingerprint("nonexistent1");
    cache.getCompactionStateByFingerprint("nonexistent2");

    Map<String, Integer> stats = cache.getAndResetStats();
    assertEquals(3, stats.get(Metric.COMPACTION_STATE_CACHE_HITS));
    assertEquals(2, stats.get(Metric.COMPACTION_STATE_CACHE_MISSES));
    assertEquals(1, stats.get(Metric.COMPACTION_STATE_CACHE_FINGERPRINTS));
  }

  @Test
  public void test_stats_resetAfterReading()
  {
    CompactionState state1 = createTestCompactionState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetCompactionStatesForPublishedSegments(stateMap);

    // Generate hits and misses
    cache.getCompactionStateByFingerprint("fingerprint1");
    cache.getCompactionStateByFingerprint("nonexistent");

    Map<String, Integer> stats1 = cache.getAndResetStats();
    assertEquals(1, stats1.get(Metric.COMPACTION_STATE_CACHE_HITS));
    assertEquals(1, stats1.get(Metric.COMPACTION_STATE_CACHE_MISSES));

    // Stats should be reset after reading
    Map<String, Integer> stats2 = cache.getAndResetStats();
    assertEquals(0, stats2.get(Metric.COMPACTION_STATE_CACHE_HITS));
    assertEquals(0, stats2.get(Metric.COMPACTION_STATE_CACHE_MISSES));
    assertEquals(1, stats2.get(Metric.COMPACTION_STATE_CACHE_FINGERPRINTS)); // Fingerprints count doesn't reset
  }

  @Test
  public void test_multipleResets_replacesCache()
  {
    CompactionState state1 = createTestCompactionState();
    CompactionState state2 = createTestCompactionState();

    // First reset
    Map<String, CompactionState> firstMap = new HashMap<>();
    firstMap.put("fingerprint1", state1);
    cache.resetCompactionStatesForPublishedSegments(firstMap);

    Optional<CompactionState> result1 = cache.getCompactionStateByFingerprint("fingerprint1");
    assertTrue(result1.isPresent());
    assertEquals(state1, result1.get());

    // Second reset with different data
    Map<String, CompactionState> secondMap = new HashMap<>();
    secondMap.put("fingerprint2", state2);
    cache.resetCompactionStatesForPublishedSegments(secondMap);

    // Old fingerprint should be gone
    Optional<CompactionState> oldResult = cache.getCompactionStateByFingerprint("fingerprint1");
    assertFalse(oldResult.isPresent());

    // New fingerprint should exist
    Optional<CompactionState> newResult = cache.getCompactionStateByFingerprint("fingerprint2");
    assertTrue(newResult.isPresent());
    assertEquals(state2, newResult.get());
  }

  @Test
  public void test_resetWithEmptyMap()
  {
    CompactionState state1 = createTestCompactionState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetCompactionStatesForPublishedSegments(stateMap);

    Optional<CompactionState> beforeReset = cache.getCompactionStateByFingerprint("fingerprint1");
    assertTrue(beforeReset.isPresent());

    // Reset with empty map
    cache.resetCompactionStatesForPublishedSegments(Collections.emptyMap());

    Optional<CompactionState> afterReset = cache.getCompactionStateByFingerprint("fingerprint1");
    assertFalse(afterReset.isPresent());

    Map<String, Integer> stats = cache.getAndResetStats();
    assertEquals(0, stats.get(Metric.COMPACTION_STATE_CACHE_FINGERPRINTS));
  }

  @Test
  public void test_addCompactionState_addsNewStateToCache()
  {
    CompactionState state = createTestCompactionState();
    String fingerprint = "test_fingerprint_123";

    // Initially, cache should not have the state
    assertEquals(Optional.empty(), cache.getCompactionStateByFingerprint(fingerprint));

    // Add the state to cache
    cache.addCompactionState(fingerprint, state);

    // Now cache should have the state
    assertEquals(Optional.of(state), cache.getCompactionStateByFingerprint(fingerprint));
  }

  @Test
  public void test_addCompactionState_withDifferentStateForSameFingerprint_updatesCache()
  {
    CompactionState state1 = createTestCompactionState();
    CompactionState state2 = new CompactionState(
        new DynamicPartitionsSpec(200, null),
        DimensionsSpec.EMPTY,
        null,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );
    String fingerprint = "same_fp";

    // Add first state
    cache.addCompactionState(fingerprint, state1);
    assertEquals(Optional.of(state1), cache.getCompactionStateByFingerprint(fingerprint));

    // Add different state with same fingerprint
    cache.addCompactionState(fingerprint, state2);

    // Cache should now have the new state
    assertEquals(Optional.of(state2), cache.getCompactionStateByFingerprint(fingerprint));
  }

  @Test
  public void test_addCompactionState_withNullFingerprint_doesNothing()
  {
    CompactionState state = createTestCompactionState();

    cache.addCompactionState(null, state);

    // Cache should remain empty
    assertEquals(0, cache.getPublishedCompactionStateMap().size());
  }

  @Test
  public void test_addCompactionState_withNullState_doesNothing()
  {
    cache.addCompactionState("some_fp", null);

    // Cache should remain empty
    assertEquals(0, cache.getPublishedCompactionStateMap().size());
  }

  private CompactionState createTestCompactionState()
  {
    return new CompactionState(
        new DynamicPartitionsSpec(100, null),
        DimensionsSpec.EMPTY,
        null,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );
  }
}
