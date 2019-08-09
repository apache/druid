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

package org.apache.druid.query.vector;

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * Class that helps vectorized query engines handle "granularity" parameters. Nonvectorized engines have it handled
 * for them by the StorageAdapter. Vectorized engines don't, because they can get efficiency gains by pushing
 * granularity handling into the engine layer.
 */
public class VectorCursorGranularizer
{
  // And a cursor that has been made from it.
  private final VectorCursor cursor;

  // Iterable that iterates over time buckets.
  private final Iterable<Interval> bucketIterable;

  // Vector selector for the "__time" column.
  @Nullable
  private final VectorValueSelector timeSelector;

  // Current time vector.
  @Nullable
  private long[] timestamps = null;

  // Offset into the vector that we should start reading from.
  private int startOffset = 0;

  // Offset into the vector that is one past the last one we should read.
  private int endOffset = 0;

  private VectorCursorGranularizer(
      VectorCursor cursor,
      Iterable<Interval> bucketIterable,
      @Nullable VectorValueSelector timeSelector
  )
  {
    this.cursor = cursor;
    this.bucketIterable = bucketIterable;
    this.timeSelector = timeSelector;
  }

  @Nullable
  public static VectorCursorGranularizer create(
      final StorageAdapter storageAdapter,
      final VectorCursor cursor,
      final Granularity granularity,
      final Interval queryInterval
  )
  {
    final DateTime minTime = storageAdapter.getMinTime();
    final DateTime maxTime = storageAdapter.getMaxTime();

    final Interval storageAdapterInterval = new Interval(minTime, granularity.bucketEnd(maxTime));
    final Interval clippedQueryInterval = queryInterval.overlap(storageAdapterInterval);

    if (clippedQueryInterval == null) {
      return null;
    }

    final Iterable<Interval> bucketIterable = granularity.getIterable(clippedQueryInterval);
    final Interval firstBucket = granularity.bucket(clippedQueryInterval.getStart());

    final VectorValueSelector timeSelector;
    if (firstBucket.contains(clippedQueryInterval)) {
      // Only one bucket, no need to read the time column.
      assert Iterables.size(bucketIterable) == 1;
      timeSelector = null;
    } else {
      // Multiple buckets, need to read the time column to know when we move from one to the next.
      timeSelector = cursor.getColumnSelectorFactory().makeValueSelector(ColumnHolder.TIME_COLUMN_NAME);
    }

    return new VectorCursorGranularizer(cursor, bucketIterable, timeSelector);
  }

  public void setCurrentOffsets(final Interval bucketInterval)
  {
    final long timeStart = bucketInterval.getStartMillis();
    final long timeEnd = bucketInterval.getEndMillis();

    int vectorSize = cursor.getCurrentVectorSize();
    endOffset = 0;

    if (timeSelector != null) {
      if (timestamps == null) {
        timestamps = timeSelector.getLongVector();
      }

      // Skip "offset" to start of bucketInterval.
      while (startOffset < vectorSize && timestamps[startOffset] < timeStart) {
        startOffset++;
      }

      // Find end of bucketInterval.
      for (endOffset = vectorSize - 1;
           endOffset >= startOffset && timestamps[endOffset] >= timeEnd;
           endOffset--) {
        // nothing needed, "for" is doing the work.
      }

      // Adjust: endOffset is now pointing at the last row to aggregate, but we want it
      // to be one _past_ the last row.
      endOffset++;
    } else {
      endOffset = vectorSize;
    }
  }

  /**
   * Return true, and advances the cursor, if it can be advanced within the current time bucket. Otherwise, returns
   * false and does nothing else.
   */
  public boolean advanceCursorWithinBucket()
  {
    if (endOffset == cursor.getCurrentVectorSize()) {
      cursor.advance();

      if (timeSelector != null && !cursor.isDone()) {
        timestamps = timeSelector.getLongVector();
      }

      startOffset = 0;

      return true;
    } else {
      return false;
    }
  }

  public Iterable<Interval> getBucketIterable()
  {
    return bucketIterable;
  }

  public int getStartOffset()
  {
    return startOffset;
  }

  public int getEndOffset()
  {
    return endOffset;
  }
}
