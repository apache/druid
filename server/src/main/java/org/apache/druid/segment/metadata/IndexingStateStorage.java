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

/**
 * Manages indexing state persistence and fingerprint generation.
 * <p>
 * Implementations may be backed by a database (like {@link SqlIndexingStateStorage}) or
 * use in-memory storage (like {@link HeapMemoryIndexingStateStorage}).
 */
public interface IndexingStateStorage
{
  /**
   * Upserts an indexing state to storage.
   * <p>
   * If a fingerprint already exists, update to reflect proper used state and timestamp.
   * If a fingerprint doesn't exist, inserts a new row with the full state payload.
   *
   * @param dataSource      The datasource name
   * @param fingerprint     The fingerprint of the compaction state
   * @param compactionState The compaction state to upsert
   * @param updateTime      The timestamp for this update
   */

  void upsertIndexingState(
      String dataSource,
      String fingerprint,
      CompactionState compactionState,
      DateTime updateTime
  );

  /**
   * Marks indexing states as unused if they are not referenced by any used segments.
   * <p>
   * This is used for cleanup operations.
   *
   * @return Number of rows updated, or 0 if not applicable
   */
  int markUnreferencedIndexingStatesAsUnused();

  /**
   * Finds all indexing state fingerprints which have been marked as unused but are
   * still referenced by some used segments. This is used for validation/reconciliation.
   * Implementations may return an empty list if not applicable.
   *
   * @return List of fingerprints, or empty list
   */
  List<String> findReferencedIndexingStateMarkedAsUnused();

  /**
   * Marks indexing states as used.
   * <p>
   * This is used for reconciliation operations to avoid deleting states that are still in use.
   *
   * @param stateFingerprints List of fingerprints to mark as used
   * @return Number of rows updated, or 0 if not applicable
   */
  int markIndexingStatesAsUsed(List<String> stateFingerprints);

  /**
   * Marks indexing states as active for a given fingerprint.
   *
   * @param stateFingerprint The fingerprint to mark as active
   * @return Number of rows updated, or 0 if not applicable
   */
  int markIndexingStatesAsActive(String stateFingerprint);

  /**
   * Deletes pending indexing states older than the given timestamp.
   * @param timestamp The cutoff timestamp in milliseconds
   * @return Number of rows deleted, or 0 if not applicable
   */
  int deletePendingIndexingStatesOlderThan(long timestamp);

  /**
   * Deletes unused indexing states older than the given timestamp.
   * <p>
   * This is used for cleanup operations.
   *
   * @param timestamp The cutoff timestamp in milliseconds
   * @return Number of rows deleted, or 0 if not applicable
   */
  int deleteUnusedIndexingStatesOlderThan(long timestamp);
}
