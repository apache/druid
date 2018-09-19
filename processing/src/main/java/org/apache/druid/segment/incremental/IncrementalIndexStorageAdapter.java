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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.Capabilities;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.Column;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 */
public class IncrementalIndexStorageAdapter implements StorageAdapter
{
  private final IncrementalIndex<?> index;

  public IncrementalIndexStorageAdapter(IncrementalIndex<?> index)
  {
    this.index = index;
  }

  @Override
  public String getSegmentIdentifier()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Interval getInterval()
  {
    return index.getInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<>(index.getDimensionNames(), String.class);
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return index.getMetricNames();
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    if (dimension.equals(Column.TIME_COLUMN_NAME)) {
      return Integer.MAX_VALUE;
    }

    IncrementalIndex.DimensionDesc desc = index.getDimension(dimension);
    if (desc == null) {
      return 0;
    }

    DimensionIndexer indexer = index.getDimension(dimension).getIndexer();
    return indexer.getCardinality();
  }

  @Override
  public int getNumRows()
  {
    return index.size();
  }

  @Override
  public DateTime getMinTime()
  {
    return index.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return index.getMaxTime();
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    IncrementalIndex.DimensionDesc desc = index.getDimension(column);
    if (desc == null) {
      return null;
    }

    DimensionIndexer indexer = desc.getIndexer();
    return indexer.getMinValue();
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    IncrementalIndex.DimensionDesc desc = index.getDimension(column);
    if (desc == null) {
      return null;
    }

    DimensionIndexer indexer = desc.getIndexer();
    return indexer.getMaxValue();
  }


  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(false).build();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return index.getCapabilities(column);
  }

  @Override
  public String getColumnTypeName(String column)
  {
    final String metricType = index.getMetricType(column);
    if (metricType != null) {
      return metricType;
    }
    ColumnCapabilities columnCapabilities = getColumnCapabilities(column);
    if (columnCapabilities != null) {
      return columnCapabilities.getType().toString();
    } else {
      return null;
    }
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return index.getMaxIngestedEventTime();
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final Granularity gran,
      final boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    if (index.isEmpty()) {
      return Sequences.empty();
    }

    final Interval dataInterval = new Interval(getMinTime(), gran.bucketEnd(getMaxTime()));

    if (!interval.overlaps(dataInterval)) {
      return Sequences.empty();
    }
    final Interval actualInterval = interval.overlap(dataInterval);
    Iterable<Interval> intervals = gran.getIterable(actualInterval);
    if (descending) {
      intervals = Lists.reverse(ImmutableList.copyOf(intervals));
    }

    return Sequences
        .simple(intervals)
        .map(i -> new IncrementalIndexCursor(virtualColumns, descending, filter, i, actualInterval, gran));
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }

  private class IncrementalIndexCursor implements Cursor
  {
    private IncrementalIndexRowHolder currEntry;
    private final ColumnSelectorFactory columnSelectorFactory;
    private final ValueMatcher filterMatcher;
    private final int maxRowIndex;
    private Iterator<IncrementalIndexRow> baseIter;
    private Iterable<IncrementalIndexRow> cursorIterable;
    private boolean emptyRange;
    private final DateTime time;
    private int numAdvanced;
    private boolean done;

    IncrementalIndexCursor(
        VirtualColumns virtualColumns,
        boolean descending,
        Filter filter,
        Interval interval,
        Interval actualInterval,
        Granularity gran
    )
    {
      currEntry = new IncrementalIndexRowHolder();
      columnSelectorFactory = new IncrementalIndexColumnSelectorFactory(index, virtualColumns, descending, currEntry);
      // Set maxRowIndex before creating the filterMatcher. See https://github.com/apache/incubator-druid/pull/6340
      maxRowIndex = index.getLastRowIndex();
      filterMatcher = filter == null ? BooleanValueMatcher.of(true) : filter.makeMatcher(columnSelectorFactory);
      numAdvanced = -1;
      final long timeStart = Math.max(interval.getStartMillis(), actualInterval.getStartMillis());
      cursorIterable = index.getFacts().timeRangeIterable(
          descending,
          timeStart,
          Math.min(actualInterval.getEndMillis(), gran.increment(interval.getStart()).getMillis())
      );
      emptyRange = !cursorIterable.iterator().hasNext();
      time = gran.toDateTime(interval.getStartMillis());

      reset();
    }

    @Override
    public ColumnSelectorFactory getColumnSelectorFactory()
    {
      return columnSelectorFactory;
    }

    @Override
    public DateTime getTime()
    {
      return time;
    }

    @Override
    public void advance()
    {
      if (!baseIter.hasNext()) {
        done = true;
        return;
      }

      while (baseIter.hasNext()) {
        BaseQuery.checkInterrupted();

        IncrementalIndexRow entry = baseIter.next();
        if (beyondMaxRowIndex(entry.getRowIndex())) {
          continue;
        }

        currEntry.set(entry);

        if (filterMatcher.matches()) {
          return;
        }
      }

      done = true;
    }

    @Override
    public void advanceUninterruptibly()
    {
      if (!baseIter.hasNext()) {
        done = true;
        return;
      }

      while (baseIter.hasNext()) {
        if (Thread.currentThread().isInterrupted()) {
          return;
        }

        IncrementalIndexRow entry = baseIter.next();
        if (beyondMaxRowIndex(entry.getRowIndex())) {
          continue;
        }

        currEntry.set(entry);

        if (filterMatcher.matches()) {
          return;
        }
      }

      done = true;
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
      return done;
    }

    @Override
    public boolean isDoneOrInterrupted()
    {
      return isDone() || Thread.currentThread().isInterrupted();
    }

    @Override
    public void reset()
    {
      baseIter = cursorIterable.iterator();

      if (numAdvanced == -1) {
        numAdvanced = 0;
      } else {
        Iterators.advance(baseIter, numAdvanced);
      }

      BaseQuery.checkInterrupted();

      boolean foundMatched = false;
      while (baseIter.hasNext()) {
        IncrementalIndexRow entry = baseIter.next();
        if (beyondMaxRowIndex(entry.getRowIndex())) {
          numAdvanced++;
          continue;
        }
        currEntry.set(entry);
        if (filterMatcher.matches()) {
          foundMatched = true;
          break;
        }

        numAdvanced++;
      }

      done = !foundMatched && (emptyRange || !baseIter.hasNext());
    }

    private boolean beyondMaxRowIndex(int rowIndex)
    {
      // ignore rows whose rowIndex is beyond the maxRowIndex
      // rows are order by timestamp, not rowIndex,
      // so we still need to go through all rows to skip rows added after cursor created
      return rowIndex > maxRowIndex;
    }
  }
}
