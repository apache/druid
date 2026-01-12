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
 * In-memory cache of compaction states used by {@link org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache}.
 * <p>
 * This cache stores compaction states for published segments polled from the metadata store.
 * It is the PRIMARY way to read compaction states in production.
 * <p>
 * The cache is populated during segment metadata cache sync operations and provides fast lookups
 * without hitting the database.
 */
@LazySingleton
public class CompactionStateCache
{
  private static final Logger log = new Logger(CompactionStateCache.class);

  /**
   * Atomically updated reference to published compaction states.
   */
  private final AtomicReference<PublishedCompactionStates> publishedCompactionStates
      = new AtomicReference<>(PublishedCompactionStates.EMPTY);

  private final AtomicInteger cacheMissCount = new AtomicInteger(0);
  private final AtomicInteger cacheHitCount = new AtomicInteger(0);

  public boolean isEnabled()
  {
    // Always enabled when this implementation is bound
    return true;
  }

  /**
   * Resets the cache with compaction states polled from the metadata store.
   * Called after each successful poll in HeapMemorySegmentMetadataCache.
   *
   * @param fingerprintToStateMap Complete map of all active compaction state fingerprints
   */
  public void resetCompactionStatesForPublishedSegments(
      Map<String, CompactionState> fingerprintToStateMap
  )
  {
    this.publishedCompactionStates.set(
        new PublishedCompactionStates(fingerprintToStateMap)
    );
    log.debug("Reset compaction state cache with [%d] fingerprints", fingerprintToStateMap.size());
  }

  /**
   * Retrieves a compaction state by its fingerprint.
   * This is the PRIMARY method for reading compaction states.
   *
   * @param fingerprint The fingerprint to look up
   * @return The compaction state, or Optional.empty() if not cached
   */
  public Optional<CompactionState> getCompactionStateByFingerprint(String fingerprint)
  {
    if (fingerprint == null) {
      return Optional.empty();
    }

    CompactionState state = publishedCompactionStates.get()
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
   * Adds or updates a single compaction state in the cache.
   * <p>
   * This is called when a new compaction state is persisted to the database via upsertCompactionState
   * to ensure the cache is immediately consistent without waiting for the next sync.
   * <p>
   * This method checks if the state is already cached before performing the atomic update.
   *
   * @param fingerprint The fingerprint key
   * @param state       The compaction state to cache
   */
  public void addCompactionState(String fingerprint, CompactionState state)
  {
    if (fingerprint == null || state == null) {
      return;
    }

    // Check if the state is already cached - avoid expensive update if not needed
    CompactionState existing = publishedCompactionStates.get()
                                                        .fingerprintToStateMap
                                                        .get(fingerprint);
    if (state.equals(existing)) {
      log.debug("Compaction state for fingerprint[%s] already cached, skipping update", fingerprint);
      return;
    }

    // State is not cached or different - perform atomic update
    publishedCompactionStates.updateAndGet(current -> {
      // Double-check in case another thread updated between our check and now
      if (state.equals(current.fingerprintToStateMap.get(fingerprint))) {
        return current;
      }

      Map<String, CompactionState> newMap = new HashMap<>(current.fingerprintToStateMap);
      newMap.put(fingerprint, state);
      return new PublishedCompactionStates(newMap);
    });

    log.debug("Added compaction state to cache for fingerprint[%s]", fingerprint);
  }

  /**
   * Gets the full cached map (immutable copy).
   * Used by HeapMemorySegmentMetadataCache for delta sync calculations.
   */
  public Map<String, CompactionState> getPublishedCompactionStateMap()
  {
    return publishedCompactionStates.get().fingerprintToStateMap;
  }

  /**
   * Clears the cache. Called when node stops being leader.
   */
  public void clear()
  {
    publishedCompactionStates.set(PublishedCompactionStates.EMPTY);
    resetStats();
  }

  /**
   * @return Summary stats for metric emission
   */
  public Map<String, Integer> getAndResetStats()
  {
    return Map.of(
        Metric.COMPACTION_STATE_CACHE_HITS, cacheHitCount.getAndSet(0),
        Metric.COMPACTION_STATE_CACHE_MISSES, cacheMissCount.getAndSet(0),
        Metric.COMPACTION_STATE_CACHE_FINGERPRINTS,
            publishedCompactionStates.get().fingerprintToStateMap.size()
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
   * Immutable snapshot of compaction states polled from DB.
   */
  private static class PublishedCompactionStates
  {
    private static final PublishedCompactionStates EMPTY =
        new PublishedCompactionStates(Map.of());

    private final Map<String, CompactionState> fingerprintToStateMap;

    private PublishedCompactionStates(Map<String, CompactionState> fingerprintToStateMap)
    {
      this.fingerprintToStateMap = Map.copyOf(fingerprintToStateMap);
    }
  }
}
