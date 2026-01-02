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

/**
 * Manages compaction state persistence and fingerprint generation.
 * <p>
 * Implementations may be backed by a database (like {@link PersistedCompactionStateManager}) or
 * use in-memory storage (like {@link HeapMemoryCompactionStateManager}).
 */
public interface CompactionStateManager
{
  /**
   * Generates a deterministic fingerprint for the given compaction state and datasource.
   * The fingerprint is a SHA-256 hash of the datasource name and serialized compaction state.
   *
   * @param compactionState The compaction configuration to fingerprint
   * @param dataSource The datasource name
   * @return A hex-encoded SHA-256 fingerprint string
   */
  String generateCompactionStateFingerprint(CompactionState compactionState, String dataSource);

  /**
   * Persists compaction states to storage.
   *
   * @param dataSource The datasource name
   * @param fingerprintToStateMap Map of fingerprints to their compaction states
   * @param updateTime The timestamp for this update
   */
  void persistCompactionState(
      String dataSource,
      Map<String, CompactionState> fingerprintToStateMap,
      DateTime updateTime
  );

  /**
   * Marks compaction states as unused if they are not referenced by any used segments.
   * This is used for cleanup operations. Implementations may choose to no-op this.
   *
   * @return Number of rows updated, or 0 if not applicable
   */
  default int markUnreferencedCompactionStatesAsUnused()
  {
    return 0;
  }

  /**
   * Finds all compaction state fingerprints which have been marked as unused but are
   * still referenced by some used segments. This is used for validation/reconciliation.
   * Implementations may return an empty list if not applicable.
   *
   * @return List of fingerprints, or empty list
   */
  default List<String> findReferencedCompactionStateMarkedAsUnused()
  {
    return List.of();
  }

  /**
   * Marks compaction states as used. This is used for reconciliation operations.
   * Implementations may choose to no-op this.
   *
   * @param stateFingerprints List of fingerprints to mark as used
   * @return Number of rows updated, or 0 if not applicable
   */
  default int markCompactionStatesAsUsed(List<String> stateFingerprints)
  {
    return 0;
  }

  /**
   * Deletes unused compaction states older than the given timestamp.
   * This is used for cleanup operations. Implementations may choose to no-op this.
   *
   * @param timestamp The cutoff timestamp in milliseconds
   * @return Number of rows deleted, or 0 if not applicable
   */
  default int deleteUnusedCompactionStatesOlderThan(long timestamp)
  {
    return 0;
  }
}
