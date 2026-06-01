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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * {@link TimeBoundaryInspector} for V10 segments. Reads min/max {@link ColumnHolder#TIME_COLUMN_NAME} values from the
 * base projection's {@link ProjectionMetadata} (persisted at write time by {@link IndexMergerBase}) so the inspector
 * can answer without reading the {@code __time} column. Falls back to the segment's data interval (inexact) when the
 * writer didn't supply min/max, for older V10 segments produced before the field existed.
 * <p>
 * The exact-bounds path is independent of segment sort order: the writer records the actual minimum and maximum
 * timestamps across all walked rows even when the segment is not time-sorted. This is a strict improvement over
 * the eager-load {@link QueryableIndexTimeBoundaryInspector}, which reports inexact bounds for non-time-sorted
 * segments because its only option is to read positions 0 and N-1 of the {@code __time} column.
 */
public final class V10TimeBoundaryInspector implements TimeBoundaryInspector
{
  /**
   * Build an inspector that reads from the given base projection metadata, falling back to {@code fallbackInterval}
   * when the writer didn't persist min/max times (e.g. pre-Phase-1.5 segments).
   */
  public static V10TimeBoundaryInspector forBaseProjection(ProjectionMetadata baseProjection, Interval fallbackInterval)
  {
    return new V10TimeBoundaryInspector(baseProjection.getMinTime(), baseProjection.getMaxTime(), fallbackInterval);
  }

  @Nullable
  private final Long minTime;
  @Nullable
  private final Long maxTime;
  private final Interval fallbackInterval;

  private V10TimeBoundaryInspector(@Nullable Long minTime, @Nullable Long maxTime, Interval fallbackInterval)
  {
    this.minTime = minTime;
    this.maxTime = maxTime;
    this.fallbackInterval = fallbackInterval;
  }

  @Override
  public DateTime getMinTime()
  {
    return minTime != null ? DateTimes.utc(minTime) : fallbackInterval.getStart();
  }

  @Override
  public DateTime getMaxTime()
  {
    return maxTime != null ? DateTimes.utc(maxTime) : fallbackInterval.getEnd();
  }

  @Override
  public boolean isMinMaxExact()
  {
    return minTime != null && maxTime != null;
  }
}
