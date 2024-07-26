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

import com.google.common.collect.Iterators;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorMaker;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.filter.ValueMatchers;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Iterator;

public class IncrementalIndexCursorMaker implements CursorMaker
{
  private final IncrementalIndexStorageAdapter storageAdapter;
  private final IncrementalIndex index;
  private final CursorBuildSpec builder;

  public IncrementalIndexCursorMaker(
      IncrementalIndexStorageAdapter storageAdapter,
      IncrementalIndex index,
      CursorBuildSpec builder
  )
  {
    this.storageAdapter = storageAdapter;
    this.index = index;
    this.builder = builder;
  }

  @Override
  public Cursor makeCursor()
  {
    if (index.isEmpty()) {
      return null;
    }

    if (builder.getQueryMetrics() != null) {
      builder.getQueryMetrics().vectorized(false);
    }

    final Interval dataInterval = new Interval(
        index.getMinTime(),
        builder.getGranularity().bucketEnd(index.getMaxTime())
    );

    if (!builder.getInterval().overlaps(dataInterval)) {
      return null;
    }
    final Interval actualInterval = builder.getInterval().overlap(dataInterval);

    return new IncrementalIndexCursor(
        storageAdapter,
        index,
        builder.getVirtualColumns(),
        builder.isDescending(),
        builder.getFilter(),
        actualInterval,
        builder.getGranularity()
    );
  }

  static class IncrementalIndexCursor implements Cursor
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
        IncrementalIndexStorageAdapter storageAdapter,
        IncrementalIndex index,
        VirtualColumns virtualColumns,
        boolean descending,
        @Nullable Filter filter,
        Interval actualInterval,
        Granularity gran
    )
    {
      currEntry = new IncrementalIndexRowHolder();
      columnSelectorFactory = new IncrementalIndexColumnSelectorFactory(
          storageAdapter,
          virtualColumns,
          descending,
          currEntry
      );
      // Set maxRowIndex before creating the filterMatcher. See https://github.com/apache/druid/pull/6340
      maxRowIndex = index.getLastRowIndex();
      filterMatcher = filter == null ? ValueMatchers.allTrue() : filter.makeMatcher(columnSelectorFactory);
      numAdvanced = -1;
      cursorIterable = index.getFacts().timeRangeIterable(
          descending,
          actualInterval.getStartMillis(),
          actualInterval.getEndMillis()
      );
      emptyRange = !cursorIterable.iterator().hasNext();
      time = gran.toDateTime(actualInterval.getStartMillis());

      reset();
    }

    @Override
    public ColumnSelectorFactory getColumnSelectorFactory()
    {
      return columnSelectorFactory;
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

        if (filterMatcher.matches(false)) {
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

        if (filterMatcher.matches(false)) {
          return;
        }
      }

      done = true;
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
        if (filterMatcher.matches(false)) {
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
