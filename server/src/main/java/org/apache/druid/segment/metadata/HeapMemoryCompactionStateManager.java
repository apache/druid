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

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link CompactionStateManager} that stores
 * compaction state fingerprints in heap memory without requiring a database.
 * <p>
 * Useful for simulations and unit tests where database persistence is not needed.
 */
public class HeapMemoryCompactionStateManager extends CompactionStateManager
{
  private final Map<String, CompactionState> fingerprintToStateMap = new ConcurrentHashMap<>();

  @Override
  public void persistCompactionState(
      final String dataSource,
      final Map<String, CompactionState> fingerprintToStateMap,
      final DateTime updateTime
  )
  {
    // Store in memory for lookup during simulations/tests
    this.fingerprintToStateMap.putAll(fingerprintToStateMap);
  }

  @Override
  @Nullable
  public CompactionState getCompactionStateByFingerprint(String fingerprint)
  {
    return fingerprintToStateMap.get(fingerprint);
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

  /**
   * Checks if a fingerprint exists in the store.
   */
  public boolean containsFingerprint(String fingerprint)
  {
    return fingerprintToStateMap.containsKey(fingerprint);
  }
}
