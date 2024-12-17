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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * Class that helps non-vectorized query engines handle "granularity" parameters. Given a set of intervals, this class
 * provides mechansims to advance a cursor to the start of an interval ({@link #advanceToBucket(Interval)}),
 * advance a cursor within a bucket interval ({@link #advanceCursorWithinBucket()}), and check if the current cursor
 * position is within the bucket {@link #currentOffsetWithinBucket()}.
 *
 * @see org.apache.druid.query.vector.VectorCursorGranularizer for vectorized query engines.
 */
public class CursorGranularizer
{
  @Nullable
  public static CursorGranularizer create(
      final Cursor cursor,
      @Nullable final TimeBoundaryInspector timeBoundaryInspector,
      final Order timeOrder,
      final Granularity granularity,
      final Interval queryInterval
  )
  {
    if (!Granularities.ALL.equals(granularity) && timeOrder == Order.NONE) {
      throw DruidException
          .forPersona(DruidException.Persona.USER)
          .ofCategory(DruidException.Category.UNSUPPORTED)
          .build("Cannot use granularity[%s] on non-time-sorted data.", granularity);
    }

    final Interval clippedQueryInterval;

    if (timeBoundaryInspector != null) {
      clippedQueryInterval = queryInterval.overlap(
          new Interval(
              timeBoundaryInspector.getMinTime(),
              granularity.bucketEnd(timeBoundaryInspector.getMaxTime())
          )
      );
    } else {
      clippedQueryInterval = queryInterval;
    }

    if (clippedQueryInterval == null) {
      return null;
    }

    Iterable<Interval> bucketIterable = granularity.getIterable(clippedQueryInterval);
    if (timeOrder == Order.DESCENDING) {
      bucketIterable = Lists.reverse(ImmutableList.copyOf(bucketIterable));
    }
    final Interval firstBucket = granularity.bucket(clippedQueryInterval.getStart());

    final ColumnValueSelector timeSelector;
    if (firstBucket.contains(clippedQueryInterval)) {
      // Only one bucket, no need to read the time column.
      assert Iterables.size(bucketIterable) == 1;
      timeSelector = null;
    } else {
      // Multiple buckets, need to read the time column to know when we move from one to the next.
      timeSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
    }

    return new CursorGranularizer(cursor, bucketIterable, timeSelector, timeOrder == Order.DESCENDING);
  }

  private final Cursor cursor;


  // Iterable that iterates over time buckets.
  private final Iterable<Interval> bucketIterable;

  // Selector for the "__time" column.
  @Nullable
  private final ColumnValueSelector timeSelector;
  private final boolean descending;

  private long currentBucketStart;
  private long currentBucketEnd;

  private CursorGranularizer(
      Cursor cursor,
      Iterable<Interval> bucketIterable,
      @Nullable ColumnValueSelector timeSelector,
      boolean descending
  )
  {
    this.cursor = cursor;
    this.bucketIterable = bucketIterable;
    this.timeSelector = timeSelector;
    this.descending = descending;
  }

  public Iterable<Interval> getBucketIterable()
  {
    return bucketIterable;
  }

  public DateTime getBucketStart()
  {
    return DateTimes.utc(currentBucketStart);
  }

  public Interval getCurrentInterval()
  {
    return Intervals.utc(currentBucketStart, currentBucketEnd);
  }

  public boolean advanceToBucket(final Interval bucketInterval)
  {
    currentBucketStart = bucketInterval.getStartMillis();
    currentBucketEnd = bucketInterval.getEndMillis();
    if (cursor.isDone()) {
      return false;
    }
    if (timeSelector == null) {
      return true;
    }
    long currentTime = timeSelector.getLong();
    if (descending) {
      while (currentTime >= currentBucketEnd && !cursor.isDone()) {
        cursor.advance();
        if (!cursor.isDone()) {
          currentTime = timeSelector.getLong();
        }
      }
    } else {
      while (currentTime < currentBucketStart && !cursor.isDone()) {
        cursor.advance();
        if (!cursor.isDone()) {
          currentTime = timeSelector.getLong();
        }
      }
    }

    return currentBucketStart <= currentTime && currentTime < currentBucketEnd;
  }

  public boolean advanceCursorWithinBucket()
  {
    if (cursor.isDone()) {
      return false;
    }
    cursor.advance();
    return currentOffsetWithinBucket();
  }

  public boolean advanceCursorWithinBucketUninterruptedly()
  {
    if (cursor.isDone()) {
      return false;
    }
    cursor.advanceUninterruptibly();
    return currentOffsetWithinBucket();
  }

  public boolean currentOffsetWithinBucket()
  {
    if (cursor.isDone()) {
      return false;
    }
    if (timeSelector == null) {
      return true;
    }
    final long currentTime = timeSelector.getLong();
    return currentBucketStart <= currentTime && currentTime < currentBucketEnd;
  }
}
