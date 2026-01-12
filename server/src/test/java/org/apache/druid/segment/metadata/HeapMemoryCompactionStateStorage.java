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

import org.apache.druid.timeline.CompactionState;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of {@link CompactionStateStorage} that stores
 * compaction state fingerprints in heap memory without requiring a database.
 * <p>
 * Useful for simulations and unit tests where database persistence is not needed.
 * Database-specific operations (cleanup, unused marking) are no-ops in this implementation.
 */
public class HeapMemoryCompactionStateStorage implements CompactionStateStorage
{
  private final ConcurrentMap<String, CompactionState> fingerprintToStateMap;

  /**
   * Creates an in-memory compaction state manager with a default deterministic mapper.
   * This is a convenience constructor for tests and simulations.
   */
  public HeapMemoryCompactionStateStorage()
  {
    this.fingerprintToStateMap = new ConcurrentHashMap<>();
  }

  @Override
  public void upsertCompactionState(
      final String dataSource,
      final String fingerprint,
      final CompactionState compactionState,
      final DateTime updateTime
  )
  {
    // Store in memory for lookup during simulations/tests
    this.fingerprintToStateMap.put(fingerprint, compactionState);
  }

  @Override
  public int markUnreferencedCompactionStatesAsUnused()
  {
    return 0;
  }

  @Override
  public List<String> findReferencedCompactionStateMarkedAsUnused()
  {
    return List.of();
  }

  @Override
  public int markCompactionStatesAsUsed(List<String> stateFingerprints)
  {
    return 0;
  }

  @Override
  public int deleteUnusedCompactionStatesOlderThan(long timestamp)
  {
    return 0;
  }

  /**
   * Gets all stored compaction states. For test verification only.
   */
  public Map<String, CompactionState> getAllStoredStates()
  {
    return Map.copyOf(fingerprintToStateMap);
  }

  /**
   * Clears all stored compaction states. Useful for test cleanup or resetting
   * state between test runs.
   */
  public void clear()
  {
    fingerprintToStateMap.clear();
  }

  /**
   * Returns the number of stored compaction state fingerprints.
   */
  public int size()
  {
    return fingerprintToStateMap.size();
  }

}
