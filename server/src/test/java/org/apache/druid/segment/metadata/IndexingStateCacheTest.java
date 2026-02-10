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

public class IndexingStateCacheTest
{
  private IndexingStateCache cache;

  @BeforeEach
  public void setUp()
  {
    cache = new IndexingStateCache();
  }

  @Test
  public void test_isEnabled_returnsTrue()
  {
    assertTrue(cache.isEnabled());
  }

  @Test
  public void test_getIndexingStateByFingerprint_emptyCache_returnsEmpty()
  {
    Optional<CompactionState> result = cache.getIndexingStateByFingerprint("nonexistent");
    assertFalse(result.isPresent());
  }

  @Test
  public void test_getIndexingStateByFingerprint_nullFingerprint_returnsEmpty()
  {
    Optional<CompactionState> result = cache.getIndexingStateByFingerprint(null);
    assertFalse(result.isPresent());
  }

  @Test
  public void test_resetIndexingStatesForPublishedSegments_andThen_getIndexingStateByFingerprint()
  {
    CompactionState state1 = createTestIndexingState();
    CompactionState state2 = createTestIndexingState();

    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);
    stateMap.put("fingerprint2", state2);

    cache.resetIndexingStatesForPublishedSegments(stateMap);

    Optional<CompactionState> result1 = cache.getIndexingStateByFingerprint("fingerprint1");
    assertTrue(result1.isPresent());
    assertEquals(state1, result1.get());

    Optional<CompactionState> result2 = cache.getIndexingStateByFingerprint("fingerprint2");
    assertTrue(result2.isPresent());
    assertEquals(state2, result2.get());

    Optional<CompactionState> result3 = cache.getIndexingStateByFingerprint("nonexistent");
    assertFalse(result3.isPresent());
  }

  @Test
  public void test_getPublishedIndexingStateMap_returnsImmutableSnapshot()
  {
    CompactionState state1 = createTestIndexingState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetIndexingStatesForPublishedSegments(stateMap);

    Map<String, CompactionState> retrieved = cache.getPublishedIndexingStateMap();
    assertEquals(1, retrieved.size());
    assertEquals(state1, retrieved.get("fingerprint1"));
  }

  @Test
  public void test_clear_emptiesCache()
  {
    CompactionState state1 = createTestIndexingState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetIndexingStatesForPublishedSegments(stateMap);

    Optional<CompactionState> beforeClear = cache.getIndexingStateByFingerprint("fingerprint1");
    assertTrue(beforeClear.isPresent());

    cache.clear();

    Optional<CompactionState> afterClear = cache.getIndexingStateByFingerprint("fingerprint1");
    assertFalse(afterClear.isPresent());

    Map<String, CompactionState> mapAfterClear = cache.getPublishedIndexingStateMap();
    assertEquals(0, mapAfterClear.size());
  }

  @Test
  public void test_stats_trackHitsAndMisses()
  {
    CompactionState state1 = createTestIndexingState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetIndexingStatesForPublishedSegments(stateMap);

    // Generate 3 hits
    cache.getIndexingStateByFingerprint("fingerprint1");
    cache.getIndexingStateByFingerprint("fingerprint1");
    cache.getIndexingStateByFingerprint("fingerprint1");

    // Generate 2 misses
    cache.getIndexingStateByFingerprint("nonexistent1");
    cache.getIndexingStateByFingerprint("nonexistent2");

    Map<String, Integer> stats = cache.getAndResetStats();
    assertEquals(3, stats.get(Metric.INDEXING_STATE_CACHE_HITS));
    assertEquals(2, stats.get(Metric.INDEXING_STATE_CACHE_MISSES));
    assertEquals(1, stats.get(Metric.INDEXING_STATE_CACHE_FINGERPRINTS));
  }

  @Test
  public void test_stats_resetAfterReading()
  {
    CompactionState state1 = createTestIndexingState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetIndexingStatesForPublishedSegments(stateMap);

    // Generate hits and misses
    cache.getIndexingStateByFingerprint("fingerprint1");
    cache.getIndexingStateByFingerprint("nonexistent");

    Map<String, Integer> stats1 = cache.getAndResetStats();
    assertEquals(1, stats1.get(Metric.INDEXING_STATE_CACHE_HITS));
    assertEquals(1, stats1.get(Metric.INDEXING_STATE_CACHE_MISSES));

    // Stats should be reset after reading
    Map<String, Integer> stats2 = cache.getAndResetStats();
    assertEquals(0, stats2.get(Metric.INDEXING_STATE_CACHE_HITS));
    assertEquals(0, stats2.get(Metric.INDEXING_STATE_CACHE_MISSES));
    assertEquals(1, stats2.get(Metric.INDEXING_STATE_CACHE_FINGERPRINTS)); // Fingerprints count doesn't reset
  }

  @Test
  public void test_multipleResets_replacesCache()
  {
    CompactionState state1 = createTestIndexingState();
    CompactionState state2 = createTestIndexingState();

    // First reset
    Map<String, CompactionState> firstMap = new HashMap<>();
    firstMap.put("fingerprint1", state1);
    cache.resetIndexingStatesForPublishedSegments(firstMap);

    Optional<CompactionState> result1 = cache.getIndexingStateByFingerprint("fingerprint1");
    assertTrue(result1.isPresent());
    assertEquals(state1, result1.get());

    // Second reset with different data
    Map<String, CompactionState> secondMap = new HashMap<>();
    secondMap.put("fingerprint2", state2);
    cache.resetIndexingStatesForPublishedSegments(secondMap);

    // Old fingerprint should be gone
    Optional<CompactionState> oldResult = cache.getIndexingStateByFingerprint("fingerprint1");
    assertFalse(oldResult.isPresent());

    // New fingerprint should exist
    Optional<CompactionState> newResult = cache.getIndexingStateByFingerprint("fingerprint2");
    assertTrue(newResult.isPresent());
    assertEquals(state2, newResult.get());
  }

  @Test
  public void test_resetWithEmptyMap()
  {
    CompactionState state1 = createTestIndexingState();
    Map<String, CompactionState> stateMap = new HashMap<>();
    stateMap.put("fingerprint1", state1);

    cache.resetIndexingStatesForPublishedSegments(stateMap);

    Optional<CompactionState> beforeReset = cache.getIndexingStateByFingerprint("fingerprint1");
    assertTrue(beforeReset.isPresent());

    // Reset with empty map
    cache.resetIndexingStatesForPublishedSegments(Collections.emptyMap());

    Optional<CompactionState> afterReset = cache.getIndexingStateByFingerprint("fingerprint1");
    assertFalse(afterReset.isPresent());

    Map<String, Integer> stats = cache.getAndResetStats();
    assertEquals(0, stats.get(Metric.INDEXING_STATE_CACHE_FINGERPRINTS));
  }

  @Test
  public void test_addIndexingState_addsNewStateToCache()
  {
    CompactionState state = createTestIndexingState();
    String fingerprint = "test_fingerprint_123";

    // Initially, cache should not have the state
    assertEquals(Optional.empty(), cache.getIndexingStateByFingerprint(fingerprint));

    // Add the state to cache
    cache.addIndexingState(fingerprint, state);

    // Now cache should have the state
    assertEquals(Optional.of(state), cache.getIndexingStateByFingerprint(fingerprint));
  }

  @Test
  public void test_addIndexingState_withDifferentStateForSameFingerprint_updatesCache()
  {
    CompactionState state1 = createTestIndexingState();
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
    cache.addIndexingState(fingerprint, state1);
    assertEquals(Optional.of(state1), cache.getIndexingStateByFingerprint(fingerprint));

    // Add different state with same fingerprint
    cache.addIndexingState(fingerprint, state2);

    // Cache should now have the new state
    assertEquals(Optional.of(state2), cache.getIndexingStateByFingerprint(fingerprint));
  }

  @Test
  public void test_addIndexingState_withNullFingerprint_doesNothing()
  {
    CompactionState state = createTestIndexingState();

    cache.addIndexingState(null, state);

    // Cache should remain empty
    assertEquals(0, cache.getPublishedIndexingStateMap().size());
  }

  @Test
  public void test_addIndexingState_withNullState_doesNothing()
  {
    cache.addIndexingState("some_fp", null);

    // Cache should remain empty
    assertEquals(0, cache.getPublishedIndexingStateMap().size());
  }

  private CompactionState createTestIndexingState()
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
