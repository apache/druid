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

import java.util.Optional;

/**
 * Provides operations for mapping between indexing state fingerprints and their corresponding states.
 * <p>
 * This interface abstracts the fingerprint generation and lookup operations, simplifying
 * dependencies and improving testability for classes that need both operations.
 */
public interface IndexingStateFingerprintMapper
{
  /**
   * Generates a deterministic fingerprint for the given indexing state and datasource.
   * <p>
   * The fingerprint is a SHA-256 hash of the datasource name and serialized indexing state that is globally unique in
   * the segment space.
   *
   * @param dataSource    The datasource name
   * @param indexingState The compaction configuration to fingerprint
   * @return A hex-encoded SHA-256 fingerprint string
   */
  String generateFingerprint(String dataSource, CompactionState indexingState);

  /**
   * Retrieves a compaction state by its fingerprint.
   *
   * @param fingerprint The fingerprint to look up
   * @return The compaction state, or Optional.empty() if not found
   */
  Optional<CompactionState> getStateForFingerprint(String fingerprint);
}
