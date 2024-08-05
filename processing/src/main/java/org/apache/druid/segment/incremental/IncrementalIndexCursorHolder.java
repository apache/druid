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
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.filter.ValueMatchers;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Iterator;

public class IncrementalIndexCursorHolder implements CursorHolder
{
  private final IncrementalIndexStorageAdapter storageAdapter;
  private final IncrementalIndex index;
  private final CursorBuildSpec spec;

  public IncrementalIndexCursorHolder(
      IncrementalIndexStorageAdapter storageAdapter,
      IncrementalIndex index,
      CursorBuildSpec spec
  )
  {
    this.storageAdapter = storageAdapter;
    this.index = index;
    this.spec = spec;
  }

  @Override
  public Cursor asCursor()
  {
    if (index.isEmpty()) {
      return null;
    }

    if (spec.getQueryMetrics() != null) {
      spec.getQueryMetrics().vectorized(false);
    }


    return new IncrementalIndexCursor(
        storageAdapter,
        index,
        spec.getVirtualColumns(),
        CursorBuildSpec.preferDescendingTimeOrder(spec.getPreferredOrdering()),
        spec.getFilter(),
        spec.getInterval()
    );
  }

  static class IncrementalIndexCursor implements Cursor
  {
    private IncrementalIndexRowHolder currEntry;
    private final ColumnSelectorFactory columnSelectorFactory;
    private final ValueMatcher filterMatcher;
    private final int maxRowIndex;
    private final IncrementalIndex.FactsHolder facts;
    private final Interval interval;
    private final boolean isDescending;
    private Iterator<IncrementalIndexRow> baseIter;
    private Iterable<IncrementalIndexRow> cursorIterable;
    private boolean emptyRange;
    private int numAdvanced;
    private boolean done;
    private DateTime markDate;
    private int markAdvanced = 0;

    IncrementalIndexCursor(
        IncrementalIndexStorageAdapter storageAdapter,
        IncrementalIndex index,
        VirtualColumns virtualColumns,
        boolean descending,
        @Nullable Filter filter,
        Interval actualInterval
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
      facts = index.getFacts();
      interval = actualInterval;
      isDescending = descending;
      markDate = isDescending ? interval.getEnd() : interval.getStart();
      cursorIterable = facts.timeRangeIterable(
          descending,
          actualInterval.getStartMillis(),
          actualInterval.getEndMillis()
      );
      emptyRange = !cursorIterable.iterator().hasNext();

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
    public void mark(DateTime mark)
    {
      markDate = mark;
      markAdvanced = numAdvanced;
    }

    @Override
    public void resetToMark()
    {
      numAdvanced = markAdvanced;
      baseIter = facts.timeRangeIterable(
          isDescending,
          isDescending ? interval.getStartMillis() : markDate.getMillis(),
          isDescending ? markDate.getMillis() : interval.getEndMillis()
      ).iterator();

      seekNextOffset();
    }

    @Override
    public void reset()
    {
      markAdvanced = 0;
      markDate = isDescending ? interval.getEnd() : interval.getStart();
      baseIter = cursorIterable.iterator();

      if (numAdvanced == -1) {
        numAdvanced = 0;
      } else {
        Iterators.advance(baseIter, numAdvanced);
      }

      seekNextOffset();
    }

    private void seekNextOffset()
    {
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
