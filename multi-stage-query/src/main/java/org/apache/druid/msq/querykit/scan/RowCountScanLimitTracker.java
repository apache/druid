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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.joda.time.Interval;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracker for a limited scan with no ordering. Any {@code rowsNeeded} rows satisfy the limit.
 */
public class RowCountScanLimitTracker extends ScanLimitTracker
{
  private final long rowsNeeded;
  private final AtomicLong rowsWritten;

  RowCountScanLimitTracker(final long rowsNeeded)
  {
    this.rowsNeeded = rowsNeeded;
    this.rowsWritten = new AtomicLong();
  }

  @VisibleForTesting
  long getRowsNeeded()
  {
    return rowsNeeded;
  }

  @Override
  public boolean isSatisfied()
  {
    return rowsWritten.get() >= rowsNeeded;
  }

  @Override
  public boolean canSkip(final Interval interval)
  {
    // Once the limit is met, no input can contribute regardless of its interval.
    return isSatisfied();
  }

  @Override
  public RowGate forCursor(final ColumnSelectorFactory columnSelectorFactory, final List<OrderBy> cursorOrdering)
  {
    // Any rows satisfy the limit, so the gate does not need to consider a row's values or the cursor's ordering.
    return new RowGate()
    {
      private long bufferedRows = 0;

      @Override
      public Decision check()
      {
        return rowsWritten.get() + bufferedRows >= rowsNeeded ? Decision.STOP : Decision.WRITE;
      }

      @Override
      public void onRowWritten()
      {
        bufferedRows++;
      }

      @Override
      public void onFrameFlushed()
      {
        rowsWritten.addAndGet(bufferedRows);
        bufferedRows = 0;
      }
    };
  }
}
