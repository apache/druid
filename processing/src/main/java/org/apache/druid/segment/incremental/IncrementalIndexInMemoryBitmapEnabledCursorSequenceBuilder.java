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

package org.apache.druid.segment.incremental;

import com.google.common.base.Function;
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
import org.apache.druid.segment.BitmapOffset;
import org.apache.druid.segment.ColumnSelectorBitmapIndexSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.FilteredOffset;
import org.apache.druid.segment.QueryableIndexCursorSequenceBuilder;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleDescendingOffset;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.historical.HistoricalCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class IncrementalIndexInMemoryBitmapEnabledCursorSequenceBuilder
{
  private final IncrementalIndexStorageAdapter storageAdapter;
  private final IncrementalIndex index;
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

  private final int maxRowIndex;

  public IncrementalIndexInMemoryBitmapEnabledCursorSequenceBuilder(
      IncrementalIndexStorageAdapter storageAdapter,
      IncrementalIndex index,
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
    this.storageAdapter = storageAdapter;
    this.index = index;
    this.interval = interval;
    this.virtualColumns = virtualColumns;
    this.filterBitmap = filterBitmap;
    this.minDataTimestamp = minDataTimestamp;
    this.maxDataTimestamp = maxDataTimestamp;
    this.descending = descending;
    this.postFilter = postFilter;
    this.bitmapIndexSelector = bitmapIndexSelector;
    // Take a snapshot what rows are there when constructing a cursor
    this.maxRowIndex = index.getLastRowIndex();
  }

  public Sequence<Cursor> build(final Granularity gran)
  {
    final Offset baseOffset;

    if (filterBitmap == null) {
      baseOffset = descending
                   ? new SimpleDescendingOffset(this.maxRowIndex + 1)
                   : new SimpleAscendingOffset(this.maxRowIndex + 1);
    } else {
      baseOffset = BitmapOffset.of(filterBitmap, descending, this.maxRowIndex + 1);
    }

    final NumericColumn timestamps = new NumericColumn()
    {
      @Override
      public int length()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public long getLongSingleValueRow(int rowNum)
      {
        return index.getFacts().getTimestamp(rowNum);
      }

      @Override
      public void close()
      {
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
      }

      @Override
      public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
      {
        throw new UnsupportedOperationException();
      }
    };

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
                // timeEnd is the max time in the current granular bucket or the actual query time end, whichever is
                // smaller
                final long timeEnd = Math.min(
                    interval.getEndMillis(),
                    gran.increment(inputInterval.getStart()).getMillis()
                );

                final Offset offset = descending ?
                                      new QueryableIndexCursorSequenceBuilder.DescendingTimestampCheckingOffset(
                                          baseOffset,
                                          timestamps,
                                          timeStart,
                                          minDataTimestamp >= timeStart
                                      ) :
                                      new QueryableIndexCursorSequenceBuilder.AscendingTimestampCheckingOffset(
                                          baseOffset,
                                          timestamps,
                                          timeEnd,
                                          // maxDataTimestamp is the max timestamp in the time column.
                                          maxDataTimestamp < timeEnd
                                      );

                final Offset baseCursorOffset = offset.clone();

                IncrementalIndexRowHolder currEntry = new IncrementalIndexRowHolder();
                final ColumnSelectorFactory columnSelectorFactory = new IncrementalIndexColumnSelectorFactory(
                    storageAdapter,
                    virtualColumns,
                    descending,
                    currEntry
                );

                final DateTime myBucket = gran.toDateTime(inputInterval.getStartMillis());

                if (postFilter == null) {
                  return new IncrementalIndexInMemoryBitmapEnabledCursor(baseCursorOffset, columnSelectorFactory, myBucket, currEntry, index);
                } else {
                  FilteredOffset filteredOffset = new FilteredOffset(
                      baseCursorOffset,
                      columnSelectorFactory,
                      descending,
                      postFilter,
                      bitmapIndexSelector
                  );
                  return new IncrementalIndexInMemoryBitmapEnabledCursor(filteredOffset, columnSelectorFactory, myBucket, currEntry, index);
                }
              }
            }
        ),
        closer
    );
  }

  private static class IncrementalIndexInMemoryBitmapEnabledCursor implements HistoricalCursor
  {
    private final Offset cursorOffset;
    private final ColumnSelectorFactory columnSelectorFactory;
    private final DateTime bucketStart;
    private final IncrementalIndexRowHolder currEntry;
    private final IncrementalIndex index;

    IncrementalIndexInMemoryBitmapEnabledCursor(Offset cursorOffset, ColumnSelectorFactory columnSelectorFactory,
                                                DateTime bucketStart, IncrementalIndexRowHolder currEntry,
                                                IncrementalIndex index)
    {
      this.cursorOffset = cursorOffset;
      this.columnSelectorFactory = columnSelectorFactory;
      this.bucketStart = bucketStart;
      this.currEntry = currEntry;
      this.index = index;

      reset();
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
      if (cursorOffset.withinBounds()) {
        currEntry.set(index.getFacts().getRow(cursorOffset.getOffset()));
      }

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
      if (cursorOffset.withinBounds()) {
        currEntry.set(index.getFacts().getRow(cursorOffset.getOffset()));
      }
    }
  }
}
