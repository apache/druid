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

package org.apache.druid.msq.querykit.scan;

import org.apache.druid.msq.querykit.InputPruner;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;

import java.util.List;

/**
 * State shared by the {@link ScanQueryFrameProcessor}s of a limited scan stage on one worker, used to stop reading
 * rows and inputs once they can no longer contribute to the result. Implementations must be thread-safe; the
 * {@link RowGate}s they hand out need not be.
 */
public abstract class ScanLimitTracker implements InputPruner
{
  /**
   * Tracker for scans with no limit that can cut reading short.
   */
  public static final ScanLimitTracker NONE = new ScanLimitTracker()
  {
    @Override
    public boolean isSatisfied()
    {
      return false;
    }

    @Override
    public RowGate forCursor(final ColumnSelectorFactory columnSelectorFactory, final List<OrderBy> cursorOrdering)
    {
      return RowGate.ALWAYS_WRITE;
    }

    @Override
    public boolean canSkip(final Interval interval)
    {
      return false;
    }
  };

  /**
   * Returns the tracker for {@code query}, or {@link #NONE} if the query has no limit that can cut reading short.
   */
  public static ScanLimitTracker create(final ScanQuery query)
  {
    if (!query.isLimited()) {
      return NONE;
    }

    final long rowsNeeded = ScanQueryFrameProcessor.getOffsetPlusLimit(query);

    if (query.getOrderBys().isEmpty()) {
      return new RowCountScanLimitTracker(rowsNeeded);
    }

    // Ingestion buckets rows by segment granularity ahead of the ordering, so its limit does not keep the rows with the
    // best __time.
    if (query.getContext().containsKey(QueryContextParameters.SQL_INSERT_SEGMENT_GRANULARITY.getName())) {
      return NONE;
    }

    final OrderBy firstOrderBy = query.getOrderBys().getFirst();
    if (rowsNeeded <= TimeOrderedScanLimitTracker.MAX_ROWS_TRACKED
        && firstOrderBy.getColumnName().equals(ColumnHolder.TIME_COLUMN_NAME)) {
      return new TimeOrderedScanLimitTracker(firstOrderBy.getOrder(), (int) rowsNeeded);
    }
    return NONE;
  }

  /**
   * Whether no more rows are needed at all. Once true, processors stop reading.
   */
  public abstract boolean isSatisfied();

  /**
   * Returns the gate for a cursor's rows. {@code columnSelectorFactory} reads the cursor's current row, and
   * {@code cursorOrdering} is the order in which the cursor returns rows, or empty if that order cannot be relied on.
   */
  public abstract RowGate forCursor(ColumnSelectorFactory columnSelectorFactory, List<OrderBy> cursorOrdering);
}
