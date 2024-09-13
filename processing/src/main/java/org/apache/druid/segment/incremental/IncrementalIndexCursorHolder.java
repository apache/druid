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
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.filter.ValueMatchers;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class IncrementalIndexCursorHolder implements CursorHolder
{
  private final IncrementalIndex index;
  private final CursorBuildSpec spec;
  private final List<OrderBy> ordering;

  public IncrementalIndexCursorHolder(
      IncrementalIndex index,
      CursorBuildSpec spec
  )
  {
    this.index = index;
    this.spec = spec;
    if (index.timePosition == 0) {
      if (Cursors.preferDescendingTimeOrdering(spec)) {
        this.ordering = Cursors.descendingTimeOrder();
      } else {
        this.ordering = Cursors.ascendingTimeOrder();
      }
    } else {
      // In principle, we could report a sort order here for certain types of fact holders; for example the
      // RollupFactsHolder would be sorted by dimensions. However, this is left for future work.
      this.ordering = Collections.emptyList();
    }
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
        index,
        spec.getVirtualColumns(),
        Cursors.getTimeOrdering(ordering),
        spec.getFilter(),
        spec.getInterval()
    );
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  static class IncrementalIndexCursor implements Cursor
  {
    private IncrementalIndexRowHolder currEntry;
    private final ColumnSelectorFactory columnSelectorFactory;
    private final ValueMatcher filterMatcher;
    private final int maxRowIndex;
    private final IncrementalIndex.FactsHolder facts;
    private Iterator<IncrementalIndexRow> baseIter;
    private Iterable<IncrementalIndexRow> cursorIterable;
    private boolean emptyRange;
    private int numAdvanced;
    private boolean done;

    IncrementalIndexCursor(
        IncrementalIndex index,
        VirtualColumns virtualColumns,
        Order timeOrder,
        @Nullable Filter filter,
        Interval actualInterval
    )
    {
      currEntry = new IncrementalIndexRowHolder();
      columnSelectorFactory = new IncrementalIndexColumnSelectorFactory(
          index,
          virtualColumns,
          timeOrder,
          currEntry
      );
      // Set maxRowIndex before creating the filterMatcher. See https://github.com/apache/druid/pull/6340
      maxRowIndex = index.getLastRowIndex();
      filterMatcher = filter == null ? ValueMatchers.allTrue() : filter.makeMatcher(columnSelectorFactory);
      numAdvanced = -1;
      facts = index.getFacts();
      cursorIterable = facts.timeRangeIterable(
          timeOrder == Order.DESCENDING,
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
