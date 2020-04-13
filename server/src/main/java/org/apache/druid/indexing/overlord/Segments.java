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

package org.apache.druid.indexing.overlord;

/**
 * This enum is used as a parameter for several methods in {@link IndexerMetadataStorageCoordinator}, specifying whether
 * only visible segments, or visible as well as overshadowed segments should be included in results.
 *
 * Visibility (and overshadowness - these terms are antonyms) may be defined on an interval (or a series of intervals).
 * Consider the following example:
 *
 * |----| I
 *   |----| S'
 *   |-------| S
 *
 * Here, I denotes an interval in question, S and S' are segments. S' is newer (has a higher version) than S.
 * Segment S is overshadowed (by S') on the interval I, though it's visible (non-overshadowed) outside of I: more
 * specifically, it's visible on the interval [end of S', end of S].
 *
 * A segment is considered visible on a series of intervals if it's visible on any of the intervals in the series. A
 * segment is considered (fully) overshadowed on a series of intervals if it's overshadowed (= non-visible) on all of
 * the intervals in the series.
 *
 * If not specified otherwise, visibility (or overshadowness) should be assumed on the interval (-inf, +inf).
 */
public enum Segments
{
  /** Specifies that only visible segments should be included in results. */
  ONLY_VISIBLE,
  /** Specifies that visible as well as overshadowed segments should be included in results. */
  INCLUDING_OVERSHADOWED
}
