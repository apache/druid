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

package org.apache.druid.segment;

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.datasourcemetadata.DataSourceMetadataResultValue;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * The difference between this class and {@link org.apache.druid.timeline.DataSegment} is that {@link
 * org.apache.druid.timeline.DataSegment} contains the segment metadata only, while this class represents the actual
 * body of segment data, queryable.
 */
@PublicApi
public interface Segment extends Closeable
{
  /**
   * Returns the {@link SegmentId} of this segment, if it is backed by a real table, otherwise returns null.
   */
  @Nullable
  SegmentId getId();

  Interval getDataInterval();

  /**
   * Request an implementation of a particular interface.
   * <p>
   * All implementations of this method should be able to provide a {@link CursorFactory}. Other interfaces are only
   * expected to be requested by callers that have specific knowledge of extra features provided by specific
   * segment types. For example, an extension might provide a custom Segment type that can offer both
   * CursorFactory and some new interface. That extension can also offer a Query that uses that new interface.
   * <p>
   * Implementations which accept classes other than known built-in interfaces such as {@link CursorFactory} are limited
   * to using those classes within the extension. This means that one extension cannot rely on the `Segment.as`
   * behavior of another extension.
   *
   * @param clazz desired interface
   * @param <T>   desired interface
   * @return instance of clazz, or null if the interface is not supported by this segment, one of the following:
   * <ul>
   *   <li> {@link CursorFactory}, to make cursors to run queries. Never null.</li>
   *   <li> {@link QueryableIndex}, index object, if this is a memory-mapped regular segment.
   *   <li> {@link IndexedTable}, table object, if this is a joinable indexed table.
   *   <li> {@link TimeBoundaryInspector}, inspector for min/max timestamps, if supported by this segment.
   *   <li> {@link PhysicalSegmentInspector}, inspector for physical segment details, if supported by this segment.
   *   <li> {@link MaxIngestedEventTimeInspector}, inspector for {@link DataSourceMetadataResultValue#getMaxIngestedEventTime()}
   *   <li> {@link TopNOptimizationInspector}, inspector containing information for topN specific optimizations
   *   <li> {@link CloseableShapeshifter}, stepping stone to {@link org.apache.druid.query.rowsandcols.RowsAndColumns}.
   *   <li> {@link BypassRestrictedSegment}, a policy-aware segment, converted from a policy-enforced segment.
   * </ul>
   */
  @SuppressWarnings({"unused", "unchecked"})
  @Nullable
  <T> T as(@Nonnull Class<T> clazz);

  default boolean isTombstone()
  {
    return false;
  }

  default String asString()
  {
    return getClass().toString();
  }
}
