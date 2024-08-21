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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Order;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.NumericColumn;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.DateTime;

/**
 * {@link TimeBoundaryInspector} for {@link QueryableIndex} that are sorted by {@link ColumnHolder#TIME_COLUMN_NAME}.
 */
public class QueryableIndexTimeBoundaryInspector implements TimeBoundaryInspector
{
  private final QueryableIndex index;
  private final boolean timeOrdered;

  private QueryableIndexTimeBoundaryInspector(final QueryableIndex index, final boolean timeOrdered)
  {
    this.index = index;
    this.timeOrdered = timeOrdered;
  }

  public static QueryableIndexTimeBoundaryInspector create(final QueryableIndex index)
  {
    return new QueryableIndexTimeBoundaryInspector(
        index,
        Cursors.getTimeOrdering(index.getOrdering()) == Order.ASCENDING
    );
  }

  @MonotonicNonNull
  private volatile DateTime minTime;

  @MonotonicNonNull
  private volatile DateTime maxTime;

  @Override
  public DateTime getMinTime()
  {
    if (minTime == null) {
      // May be called a few times in parallel when first populating minTime, but this is benign, so allow it.
      populateMinMaxTime();
    }

    return minTime;
  }

  @Override
  public DateTime getMaxTime()
  {
    if (maxTime == null) {
      // May be called a few times in parallel when first populating maxTime, but this is benign, so allow it.
      populateMinMaxTime();
    }

    return maxTime;
  }

  @Override
  public boolean isMinMaxExact()
  {
    return timeOrdered;
  }

  private void populateMinMaxTime()
  {
    if (timeOrdered) {
      // Compute and cache minTime, maxTime.
      final ColumnHolder columnHolder = index.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME);
      try (NumericColumn column = (NumericColumn) columnHolder.getColumn()) {
        this.minTime = DateTimes.utc(column.getLongSingleValueRow(0));
        this.maxTime = DateTimes.utc(column.getLongSingleValueRow(column.length() - 1));
      }
    } else {
      // Use metadata. (Will be inexact.)
      this.minTime = index.getDataInterval().getStart();
      this.maxTime = index.getDataInterval().getEnd();
    }
  }
}
