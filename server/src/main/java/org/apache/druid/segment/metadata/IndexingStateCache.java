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

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.CompactionState;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory cache of indexing states used by {@link org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache}.
 * <p>
 * This cache stores indexing states for published segments polled from the metadata store.
 * It is the primary way to read indexing states in production.
 * <p>
 * The cache is populated during segment metadata cache sync operations and provides fast lookups
 * without hitting the database.
 */
@LazySingleton
public class IndexingStateCache
{
  private static final Logger log = new Logger(IndexingStateCache.class);

  /**
   * Atomically updated reference to published indexing states.
   */
  private final AtomicReference<PublishedIndexingStates> publishedIndexingStates
      = new AtomicReference<>(PublishedIndexingStates.EMPTY);

  private final AtomicInteger cacheMissCount = new AtomicInteger(0);
  private final AtomicInteger cacheHitCount = new AtomicInteger(0);

  public boolean isEnabled()
  {
    // Always enabled when this implementation is bound
    return true;
  }

  /**
   * Resets the cache with indexing states polled from the metadata store.
   * Called after each successful poll in HeapMemorySegmentMetadataCache.
   *
   * @param fingerprintToStateMap Complete fp:state map of all active indexing state fingerprints
   */
  public void resetIndexingStatesForPublishedSegments(
      Map<String, CompactionState> fingerprintToStateMap
  )
  {
    this.publishedIndexingStates.set(
        new PublishedIndexingStates(fingerprintToStateMap)
    );
    log.debug("Reset indexing state cache with [%d] fingerprints", fingerprintToStateMap.size());
  }

  /**
   * Retrieves an indexing state by its fingerprint.
   * This is the indexing method for reading indexing states.
   *
   * @param fingerprint The fingerprint to look up
   * @return The cached indexing state, or Optional.empty() if not cached
   */
  public Optional<CompactionState> getIndexingStateByFingerprint(String fingerprint)
  {
    if (fingerprint == null) {
      return Optional.empty();
    }

    CompactionState state = publishedIndexingStates.get()
                                                     .fingerprintToStateMap
                                                     .get(fingerprint);
    if (state == null) {
      cacheMissCount.incrementAndGet();
      return Optional.empty();
    } else {
      cacheHitCount.incrementAndGet();
      return Optional.of(state);
    }
  }

  /**
   * Adds or updates a single indexing state in the cache.
   * <p>
   * This is called when a new compaction state is persisted to the database via upsertIndexingState
   * to ensure the cache is immediately consistent without waiting for the next sync.
   * <p>
   * This method checks if the state is already cached before performing the atomic update.
   *
   * @param fingerprint The fingerprint key
   * @param state       The indexing state to cache
   */
  public void addIndexingState(String fingerprint, CompactionState state)
  {
    if (fingerprint == null || state == null) {
      return;
    }

    // Check if the state is already cached - avoid expensive update if not needed
    CompactionState existing = publishedIndexingStates.get()
                                                        .fingerprintToStateMap
                                                        .get(fingerprint);
    if (state.equals(existing)) {
      log.debug("Indexing state for fingerprint[%s] already cached, skipping update", fingerprint);
      return;
    }

    // State is not cached or different - perform atomic update
    publishedIndexingStates.updateAndGet(current -> {
      // Double-check in case another thread updated between our check and now
      if (state.equals(current.fingerprintToStateMap.get(fingerprint))) {
        return current;
      }

      Map<String, CompactionState> newMap = new HashMap<>(current.fingerprintToStateMap);
      newMap.put(fingerprint, state);
      return new PublishedIndexingStates(newMap);
    });

    log.debug("Added indexing state to cache for fingerprint[%s]", fingerprint);
  }

  /**
   * Gets the full cached map (immutable copy).
   * Used by HeapMemorySegmentMetadataCache for delta sync calculations.
   */
  public Map<String, CompactionState> getPublishedIndexingStateMap()
  {
    return publishedIndexingStates.get().fingerprintToStateMap;
  }

  /**
   * Clears the cache. Called when node stops being leader.
   */
  public void clear()
  {
    publishedIndexingStates.set(PublishedIndexingStates.EMPTY);
    resetStats();
  }

  /**
   * @return Summary stats for metric emission
   */
  public Map<String, Integer> getAndResetStats()
  {
    return Map.of(
        Metric.INDEXING_STATE_CACHE_HITS, cacheHitCount.getAndSet(0),
        Metric.INDEXING_STATE_CACHE_MISSES, cacheMissCount.getAndSet(0),
        Metric.INDEXING_STATE_CACHE_FINGERPRINTS,
        publishedIndexingStates.get().fingerprintToStateMap.size()
    );
  }

  /**
   * Resets hit/miss stats.
   */
  private void resetStats()
  {
    cacheHitCount.set(0);
    cacheMissCount.set(0);
  }

  /**
   * Immutable snapshot of indexing states polled from DB.
   */
  private static class PublishedIndexingStates
  {
    private static final PublishedIndexingStates EMPTY =
        new PublishedIndexingStates(Map.of());

    private final Map<String, CompactionState> fingerprintToStateMap;

    private PublishedIndexingStates(Map<String, CompactionState> fingerprintToStateMap)
    {
      this.fingerprintToStateMap = Map.copyOf(fingerprintToStateMap);
    }
  }
}
