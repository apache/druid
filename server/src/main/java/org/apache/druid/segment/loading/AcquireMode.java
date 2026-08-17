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

package org.apache.druid.segment.loading;

/**
 * How {@link SegmentCacheManager#acquireSegment} and {@link SegmentCacheManager#acquireCachedSegment} materialize a
 * segment. Both modes return an open reference that callers must close when finished; they differ only in how much
 * of the segment is downloaded before the reference is handed back.
 */
public enum AcquireMode
{
  /**
   * Download the entire segment up front, so the returned {@link org.apache.druid.segment.Segment} is
   * fully-materialized: every column is reachable synchronously by the time a cursor is built. This is the right
   * choice for callers that use the synchronous {@link org.apache.druid.segment.CursorFactory#makeCursorHolder}.
   */
  FULL,

  /**
   * Mount only what is needed to serve the segment's schema (for a partial-eligible segment, just the metadata header).
   * Callers must therefore use the async API such as
   * {@link org.apache.druid.segment.CursorFactory#makeCursorHolderAsync} where column/bundle data is fetched on demand
   * at cursor-build.
   * <p>
   * Degrades to {@link #FULL} behavior when the segment's deep-storage layout cannot supply range reads (e.g. a
   * non-V10 or zipped segment) or when partial downloads are disabled by configuration.
   */
  PARTIAL
}
