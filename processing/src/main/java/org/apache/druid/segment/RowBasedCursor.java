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

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.function.ToLongFunction;

/**
 * A {@link Cursor} that is based on a stream of objects. Generally created by a {@link RowBasedStorageAdapter}.
 *
 * @see RowBasedSegment#RowBasedSegment for implementation notes
 */
public class RowBasedCursor<RowType> implements Cursor
{
  private final RowWalker<RowType> rowWalker;
  private final ToLongFunction<RowType> timestampFunction;
  private final Interval interval;
  private final boolean descending;
  private final DateTime cursorTime;
  private final ColumnSelectorFactory columnSelectorFactory;
  private final ValueMatcher valueMatcher;

  RowBasedCursor(
      final RowWalker<RowType> rowWalker,
      final RowAdapter<RowType> rowAdapter,
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final Granularity gran,
      final boolean descending,
      final RowSignature rowSignature
  )
  {
    this.rowWalker = rowWalker;
    this.timestampFunction = rowAdapter.timestampFunction();
    this.interval = interval;
    this.descending = descending;
    this.cursorTime = gran.toDateTime(interval.getStartMillis());
    this.columnSelectorFactory = virtualColumns.wrap(
        RowBasedColumnSelectorFactory.create(
            rowAdapter,
            rowWalker::currentRow,
            rowSignature,
            false
        )
    );

    if (filter == null) {
      this.valueMatcher = BooleanValueMatcher.of(true);
    } else {
      this.valueMatcher = filter.makeMatcher(this.columnSelectorFactory);
    }

    rowWalker.skipToDateTime(descending ? interval.getEnd().minus(1) : interval.getStart(), descending);
    advanceToMatchingRow();
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return columnSelectorFactory;
  }

  @Override
  public DateTime getTime()
  {
    return cursorTime;
  }

  @Override
  public void advance()
  {
    advanceUninterruptibly();
    BaseQuery.checkInterrupted();
  }

  @Override
  public void advanceUninterruptibly()
  {
    rowWalker.advance();
    advanceToMatchingRow();
  }

  @Override
  public boolean isDone()
  {
    return rowWalker.isDone() || !interval.contains(timestampFunction.applyAsLong(rowWalker.currentRow()));
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    rowWalker.reset();
    rowWalker.skipToDateTime(descending ? interval.getEnd().minus(1) : interval.getStart(), descending);
    advanceToMatchingRow();
  }

  private void advanceToMatchingRow()
  {
    while (!isDone() && !valueMatcher.matches()) {
      rowWalker.advance();
    }
  }
}
