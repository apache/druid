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

import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 *
 */
@Deprecated
@PublicApi
public interface StorageAdapter extends ColumnInspector
{
  /**
   * @deprecated Use {@link Segment#asCursorFactory()} and then {@link CursorFactory#makeCursorHolder(CursorBuildSpec)}
   * and call {@link CursorHolder#canVectorize()} instead.
   */
  @Deprecated
  default boolean canVectorize(
      @Nullable Filter filter,
      VirtualColumns virtualColumns,
      boolean descending
  )
  {
    throw DruidException.defensive(
        "canVectorize is no longer supported, use Segment.asCursorFactory().makeCursorHolder(..).canVectorize() instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#asCursorFactory()} and then {@link CursorFactory#makeCursorHolder(CursorBuildSpec)}
   * and call {@link CursorHolder#asCursor()} instead.
   */
  @Deprecated
  default Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    throw DruidException.defensive(
        "makeCursors is no longer supported, use Segment.asCursorFactory().makeCursorHolder(..).asCursor() instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#asCursorFactory()} and then {@link CursorFactory#makeCursorHolder(CursorBuildSpec)}
   * and call {@link CursorHolder#asVectorCursor()} instead.
   */
  @Deprecated
  @Nullable
  default VectorCursor makeVectorCursor(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      boolean descending,
      int vectorSize,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    throw DruidException.defensive(
        "makeVectorCursor is no longer supported, use Segment.asCursorFactory().makeCursorHolder(..).asVectorCursor() instead"
    );
  }

  /**
   * @deprecated Callers should use {@link Segment#getDataInterval()} instead.
   */
  @Deprecated
  default Interval getInterval()
  {
    throw DruidException.defensive(
        "getInterval is no longer supported, use Segment.getDataInterval() instead."
    );
  }

  /**
   * @deprecated Callers should use {@link Segment#as(Class)} to construct a {@link Metadata} instead.
   */
  @Deprecated
  default Indexed<String> getAvailableDimensions()
  {
    throw DruidException.defensive(
        "getAvailableDimensions is no longer supported, use Segment.getRowSignature() and or Segment.as(PhysicalSegmentInspector.class) instead"
    );
  }

  /**
   * @deprecated Callers should use {@link Segment#as(Class)} to construct a {@link Metadata} if available and check
   * {@link Metadata#getAggregators()} instead.
   */
  @Deprecated
  default Iterable<String> getAvailableMetrics()
  {
    throw DruidException.defensive(
        "getAvailableMetrics is no longer supported, use Segment.as(PhysicalSegmentInspector.class) instead"
    );
  }

  /**
   * @deprecated use {@link Segment#asCursorFactory()} and {@link CursorFactory#getRowSignature()} instead.
   */
  @Deprecated
  default RowSignature getRowSignature()
  {
    throw DruidException.defensive(
        "getRowSignature is no longer supported, use Segment.asCursorFactory().getRowSignature() instead"
    );
  }

  /**
   * @deprecated Callers should use {@link Segment#as(Class)} to construct a {@link PhysicalSegmentInspector} if
   * available and call {@link PhysicalSegmentInspector#getDimensionCardinality(String)} instead.
   */
  @Deprecated
  default int getDimensionCardinality(String column)
  {
    throw DruidException.defensive(
        "getDimensionCardinality is no longer supported, use Segment.as(SegmentAnalysisInspector.class) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#as(Class)} to get a {@link TimeBoundaryInspector} if available and call
   * {@link TimeBoundaryInspector#getMinTime()} instead.
   */
  @Deprecated
  default DateTime getMinTime()
  {
    throw DruidException.defensive(
        "getMinTime is no longer supported, use Segment.as(TimeBoundaryInspector.class) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#as(Class)} to get a {@link TimeBoundaryInspector} if available and call
   * {@link TimeBoundaryInspector#getMaxTime()} instead.
   */
  @Deprecated
  default DateTime getMaxTime()
  {
    throw DruidException.defensive(
        "getMaxTime is no longer supported, use Segment.as(TimeBoundaryInspector.class) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#as(Class)} to get a {@link PhysicalSegmentInspector} if available and call
   * {@link PhysicalSegmentInspector#getMinValue(String)}
   */
  @Deprecated
  @Nullable
  default Comparable getMinValue(String column)
  {
    throw DruidException.defensive(
        "getMinValue is no longer supported, use Segment.as(SegmentAnalysisInspector.class) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#as(Class)} to get a {@link PhysicalSegmentInspector} if available and call
   * {@link PhysicalSegmentInspector#getMaxValue(String)}
   */
  @Deprecated
  @Nullable
  default Comparable getMaxValue(String column)
  {
    throw DruidException.defensive(
        "getMaxValue is no longer supported, use Segment.as(SegmentAnalysisInspector.class) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#asCursorFactory()} and then {@link CursorFactory#getColumnCapabilities(String)}
   * instead.
   */
  @Deprecated
  @Override
  @Nullable
  default ColumnCapabilities getColumnCapabilities(String column)
  {
    throw DruidException.defensive(
        "getColumnCapabilities is no longer supported, use Segment.asCursorFactory().getColumnCapabilities(..) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#as(Class)} to get a {@link PhysicalSegmentInspector} if available then call
   * {@link PhysicalSegmentInspector#getNumRows()} instead.
   */
  @Deprecated
  default int getNumRows()
  {
    throw DruidException.defensive(
        "getNumRows is no longer supported, use Segment.as(PhysicalSegmentInspector.class) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#as(Class)} to get a {@link MaxIngestedEventTimeInspector} if available and call
   * {@link MaxIngestedEventTimeInspector#getMaxIngestedEventTime()} instead.
   */
  @Deprecated
  default DateTime getMaxIngestedEventTime()
  {
    throw DruidException.defensive(
        "getMaxIngestedEventTime is no longer supported, use Segment.as(MaxIngestedEventTimeInspector.class) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#as(Class)} to fetch a {@link Metadata} if available
   */
  @Deprecated
  @Nullable
  default Metadata getMetadata()
  {
    throw DruidException.defensive(
        "getMetadata is no longer supported, use Segment.as(PhysicalSegmentInspector.class) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#as(Class)} to get a {@link TopNOptimizationInspector} if available and call
   * {@link TopNOptimizationInspector#areAllDictionaryIdsPresent()} instead.
   */
  @Deprecated
  default boolean hasBuiltInFilters()
  {
    throw DruidException.defensive(
        "hasBuiltInFilters is no longer supported, use Segment.as(FilteredSegmentInspector.class) instead"
    );
  }

  /**
   * @deprecated Use {@link Segment#isTombstone()}
   */
  @Deprecated
  default boolean isFromTombstone()
  {
    throw DruidException.defensive(
        "isFromTombstone is no longer supported, use Segment.isTombstone instead"
    );
  }
}
