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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.historical.HistoricalCursor;
import org.apache.druid.segment.vector.BitmapVectorOffset;
import org.apache.druid.segment.vector.FilteredVectorOffset;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.QueryableIndexVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorOffset;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class QueryableIndexCursorSequenceBuilder
{
  private final QueryableIndex index;
  private final Interval interval;
  private final VirtualColumns virtualColumns;
  @Nullable
  private final ImmutableBitmap filterBitmap;
  private final long minDataTimestamp;
  private final long maxDataTimestamp;
  private final boolean descending;
  @Nullable
  private final Filter postFilter;
  @Nullable
  private final ColumnSelectorBitmapIndexSelector bitmapIndexSelector;

  public QueryableIndexCursorSequenceBuilder(
      QueryableIndex index,
      Interval interval,
      VirtualColumns virtualColumns,
      @Nullable ImmutableBitmap filterBitmap,
      long minDataTimestamp,
      long maxDataTimestamp,
      boolean descending,
      @Nullable Filter postFilter,
      @Nullable ColumnSelectorBitmapIndexSelector bitmapIndexSelector
  )
  {
    this.index = index;
    this.interval = interval;
    this.virtualColumns = virtualColumns;
    this.filterBitmap = filterBitmap;
    this.minDataTimestamp = minDataTimestamp;
    this.maxDataTimestamp = maxDataTimestamp;
    this.descending = descending;
    this.postFilter = postFilter;
    this.bitmapIndexSelector = bitmapIndexSelector;
  }

  public Sequence<Cursor> build(final Granularity gran)
  {
    final Offset baseOffset;

    if (filterBitmap == null) {
      baseOffset = descending
                   ? new SimpleDescendingOffset(index.getNumRows())
                   : new SimpleAscendingOffset(index.getNumRows());
    } else {
      baseOffset = BitmapOffset.of(filterBitmap, descending, index.getNumRows());
    }

    // Column caches shared amongst all cursors in this sequence.
    final Map<String, BaseColumn> columnCache = new HashMap<>();

    final NumericColumn timestamps = (NumericColumn) index.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME).getColumn();

    final Closer closer = Closer.create();
    closer.register(timestamps);

    Iterable<Interval> iterable = gran.getIterable(interval);
    if (descending) {
      iterable = Lists.reverse(ImmutableList.copyOf(iterable));
    }

    return Sequences.withBaggage(
        Sequences.map(
            Sequences.simple(iterable),
            new Function<Interval, Cursor>()
            {
              @Override
              public Cursor apply(final Interval inputInterval)
              {
                final long timeStart = Math.max(interval.getStartMillis(), inputInterval.getStartMillis());
                final long timeEnd = Math.min(
                    interval.getEndMillis(),
                    gran.increment(inputInterval.getStart()).getMillis()
                );

                if (descending) {
                  for (; baseOffset.withinBounds(); baseOffset.increment()) {
                    if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) < timeEnd) {
                      break;
                    }
                  }
                } else {
                  for (; baseOffset.withinBounds(); baseOffset.increment()) {
                    if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) >= timeStart) {
                      break;
                    }
                  }
                }

                final Offset offset = descending ?
                                      new DescendingTimestampCheckingOffset(
                                          baseOffset,
                                          timestamps,
                                          timeStart,
                                          minDataTimestamp >= timeStart
                                      ) :
                                      new AscendingTimestampCheckingOffset(
                                          baseOffset,
                                          timestamps,
                                          timeEnd,
                                          maxDataTimestamp < timeEnd
                                      );


                final Offset baseCursorOffset = offset.clone();
                final ColumnSelectorFactory columnSelectorFactory = new QueryableIndexColumnSelectorFactory(
                    index,
                    virtualColumns,
                    descending,
                    closer,
                    baseCursorOffset.getBaseReadableOffset(),
                    columnCache
                );
                final DateTime myBucket = gran.toDateTime(inputInterval.getStartMillis());

                if (postFilter == null) {
                  return new QueryableIndexCursor(baseCursorOffset, columnSelectorFactory, myBucket);
                } else {
                  FilteredOffset filteredOffset = new FilteredOffset(
                      baseCursorOffset,
                      columnSelectorFactory,
                      descending,
                      postFilter,
                      bitmapIndexSelector
                  );
                  return new QueryableIndexCursor(filteredOffset, columnSelectorFactory, myBucket);
                }

              }
            }
        ),
        closer
    );
  }

  public VectorCursor buildVectorized(final int vectorSize)
  {
    // Sanity check - matches QueryableIndexStorageAdapter.canVectorize
    Preconditions.checkState(virtualColumns.size() == 0, "virtualColumns.size == 0");
    Preconditions.checkState(!descending, "!descending");

    final Map<String, BaseColumn> columnCache = new HashMap<>();
    final Closer closer = Closer.create();

    NumericColumn timestamps = null;

    final int startOffset;
    final int endOffset;

    if (interval.getStartMillis() > minDataTimestamp) {
      timestamps = (NumericColumn) index.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME).getColumn();
      closer.register(timestamps);

      startOffset = timeSearch(timestamps, interval.getStartMillis(), 0, index.getNumRows());
    } else {
      startOffset = 0;
    }

    if (interval.getEndMillis() <= maxDataTimestamp) {
      if (timestamps == null) {
        timestamps = (NumericColumn) index.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME).getColumn();
        closer.register(timestamps);
      }

      endOffset = timeSearch(timestamps, interval.getEndMillis(), startOffset, index.getNumRows());
    } else {
      endOffset = index.getNumRows();
    }

    final VectorOffset baseOffset =
        filterBitmap == null
        ? new NoFilterVectorOffset(vectorSize, startOffset, endOffset)
        : new BitmapVectorOffset(vectorSize, filterBitmap, startOffset, endOffset);

    if (postFilter == null) {
      return new QueryableIndexVectorCursor(index, baseOffset, closer, columnCache, vectorSize);
    } else {
      // baseColumnSelectorFactory using baseOffset is the column selector for filtering.
      final VectorColumnSelectorFactory baseColumnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
          index,
          baseOffset,
          closer,
          columnCache
      );

      final VectorOffset filteredOffset = FilteredVectorOffset.create(
          baseOffset,
          baseColumnSelectorFactory,
          postFilter
      );

      // Now create the cursor and column selector that will be returned to the caller.
      //
      // There is an inefficiency with how we do things here: this cursor (the one that will be provided to the
      // caller) does share a columnCache with "baseColumnSelectorFactory", but it *doesn't* share vector data. This
      // means that if the caller wants to read from a column that is also used for filtering, the underlying column
      // object will get hit twice for some of the values (anything that matched the filter). This is probably most
      // noticeable if it causes thrashing of decompression buffers due to out-of-order reads. I haven't observed
      // this directly but it seems possible in principle.
      return new QueryableIndexVectorCursor(index, filteredOffset, closer, columnCache, vectorSize);
    }
  }

  /**
   * Search the time column using binary search. Benchmarks on various other approaches (linear search, binary
   * search that switches to linear at various closeness thresholds) indicated that a pure binary search worked best.
   *
   * @param timeColumn          the column
   * @param timestamp           the timestamp to search for
   * @param startIndex          first index to search, inclusive
   * @param endIndex            last index to search, exclusive
   *
   * @return first index that has a timestamp equal to, or greater, than "timestamp"
   */
  @VisibleForTesting
  static int timeSearch(
      final NumericColumn timeColumn,
      final long timestamp,
      final int startIndex,
      final int endIndex
  )
  {
    final long prevTimestamp = timestamp - 1;

    // Binary search for prevTimestamp.
    int minIndex = startIndex;
    int maxIndex = endIndex - 1;

    while (minIndex <= maxIndex) {
      final int currIndex = (minIndex + maxIndex) >>> 1;
      final long currValue = timeColumn.getLongSingleValueRow(currIndex);

      if (currValue < prevTimestamp) {
        minIndex = currIndex + 1;
      } else if (currValue > prevTimestamp) {
        maxIndex = currIndex - 1;
      } else {
        // The value at currIndex is prevTimestamp.
        minIndex = currIndex;
        break;
      }
    }

    // Do linear search for the actual timestamp, then return.
    for (; minIndex < endIndex; minIndex++) {
      final long currValue = timeColumn.getLongSingleValueRow(minIndex);
      if (currValue >= timestamp) {
        return minIndex;
      }
    }

    // Not found.
    return endIndex;
  }

  private static class QueryableIndexVectorCursor implements VectorCursor
  {
    private final Closer closer;
    private final int vectorSize;
    private final VectorOffset offset;
    private final VectorColumnSelectorFactory columnSelectorFactory;

    public QueryableIndexVectorCursor(
        final QueryableIndex index,
        final VectorOffset offset,
        final Closer closer,
        final Map<String, BaseColumn> columnCache,
        final int vectorSize
    )
    {
      this.offset = offset;
      this.closer = closer;
      this.vectorSize = vectorSize;
      this.columnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(index, offset, closer, columnCache);
    }

    @Override
    public int getMaxVectorSize()
    {
      return vectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return offset.getCurrentVectorSize();
    }

    @Override
    public VectorColumnSelectorFactory getColumnSelectorFactory()
    {
      return columnSelectorFactory;
    }

    @Override
    public void advance()
    {
      offset.advance();
      BaseQuery.checkInterrupted();
    }

    @Override
    public boolean isDone()
    {
      return offset.isDone();
    }

    @Override
    public void reset()
    {
      offset.reset();
    }

    @Override
    public void close()
    {
      try {
        closer.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class QueryableIndexCursor implements HistoricalCursor
  {
    private final Offset cursorOffset;
    private final ColumnSelectorFactory columnSelectorFactory;
    private final DateTime bucketStart;

    QueryableIndexCursor(Offset cursorOffset, ColumnSelectorFactory columnSelectorFactory, DateTime bucketStart)
    {
      this.cursorOffset = cursorOffset;
      this.columnSelectorFactory = columnSelectorFactory;
      this.bucketStart = bucketStart;
    }

    @Override
    public Offset getOffset()
    {
      return cursorOffset;
    }

    @Override
    public ColumnSelectorFactory getColumnSelectorFactory()
    {
      return columnSelectorFactory;
    }

    @Override
    public DateTime getTime()
    {
      return bucketStart;
    }

    @Override
    public void advance()
    {
      cursorOffset.increment();
      // Must call BaseQuery.checkInterrupted() after cursorOffset.increment(), not before, because
      // FilteredOffset.increment() is a potentially long, not an "instant" operation (unlike to all other subclasses
      // of Offset) and it returns early on interruption, leaving itself in an illegal state. We should not let
      // aggregators, etc. access this illegal state and throw a QueryInterruptedException by calling
      // BaseQuery.checkInterrupted().
      BaseQuery.checkInterrupted();
    }

    @Override
    public void advanceUninterruptibly()
    {
      cursorOffset.increment();
    }

    @Override
    public void advanceTo(int offset)
    {
      int count = 0;
      while (count < offset && !isDone()) {
        advance();
        count++;
      }
    }

    @Override
    public boolean isDone()
    {
      return !cursorOffset.withinBounds();
    }

    @Override
    public boolean isDoneOrInterrupted()
    {
      return isDone() || Thread.currentThread().isInterrupted();
    }

    @Override
    public void reset()
    {
      cursorOffset.reset();
    }
  }


  public abstract static class TimestampCheckingOffset extends Offset
  {
    final Offset baseOffset;
    final NumericColumn timestamps;
    final long timeLimit;
    final boolean allWithinThreshold;

    TimestampCheckingOffset(
        Offset baseOffset,
        NumericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      this.baseOffset = baseOffset;
      this.timestamps = timestamps;
      this.timeLimit = timeLimit;
      // checks if all the values are within the Threshold specified, skips timestamp lookups and checks if all values are within threshold.
      this.allWithinThreshold = allWithinThreshold;
    }

    @Override
    public int getOffset()
    {
      return baseOffset.getOffset();
    }

    @Override
    public boolean withinBounds()
    {
      if (!baseOffset.withinBounds()) {
        return false;
      }
      if (allWithinThreshold) {
        return true;
      }
      return timeInRange(timestamps.getLongSingleValueRow(baseOffset.getOffset()));
    }

    @Override
    public void reset()
    {
      baseOffset.reset();
    }

    @Override
    public ReadableOffset getBaseReadableOffset()
    {
      return baseOffset.getBaseReadableOffset();
    }

    protected abstract boolean timeInRange(long current);

    @Override
    public void increment()
    {
      baseOffset.increment();
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Offset clone()
    {
      throw new IllegalStateException("clone");
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("baseOffset", baseOffset);
      inspector.visit("timestamps", timestamps);
      inspector.visit("allWithinThreshold", allWithinThreshold);
    }
  }

  public static class AscendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    AscendingTimestampCheckingOffset(
        Offset baseOffset,
        NumericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      super(baseOffset, timestamps, timeLimit, allWithinThreshold);
    }

    @Override
    protected final boolean timeInRange(long current)
    {
      return current < timeLimit;
    }

    @Override
    public String toString()
    {
      return (baseOffset.withinBounds() ? timestamps.getLongSingleValueRow(baseOffset.getOffset()) : "OOB") +
             "<" + timeLimit + "::" + baseOffset;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Offset clone()
    {
      return new AscendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit, allWithinThreshold);
    }
  }

  public static class DescendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    DescendingTimestampCheckingOffset(
        Offset baseOffset,
        NumericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      super(baseOffset, timestamps, timeLimit, allWithinThreshold);
    }

    @Override
    protected final boolean timeInRange(long current)
    {
      return current >= timeLimit;
    }

    @Override
    public String toString()
    {
      return timeLimit + ">=" +
             (baseOffset.withinBounds() ? timestamps.getLongSingleValueRow(baseOffset.getOffset()) : "OOB") +
             "::" + baseOffset;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Offset clone()
    {
      return new DescendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit, allWithinThreshold);
    }
  }
}
