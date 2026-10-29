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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.commons.numbers.arrays.Selection;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;

/**
 * Tracker for a limited scan whose first ordering column is {@code __time}.
 * <p>
 * Maintains a threshold such that at least offset + limit written rows are as good or better than it; rows and inputs
 * that cannot beat (or tie) it cannot contribute to the result and thus can be skipped.
 * <ul>
 * <li>The threshold for ASCENDING scans is the largest timestamp in the current selection of rows.</li>
 * <li>The threshold for DESCENDING scans is the smallest timestamp in the current selection of rows.</li>
 * </ul>
 * <p>
 * Gates timestamps to the tracker in batches, and the tracker appends those that beat the threshold to a buffer. When
 * the buffer fills, a selection keeps the best offset + limit timestamps. The worst timestamp in that selection becomes
 * the new threshold. By design, the threshold lags slightly behind the rows written, which does not affect correctness
 * but may slightly reduce pruning effectiveness.
 */
public class TimeOrderedScanLimitTracker extends ScanLimitTracker
{
  private static final int INITIAL_CAPACITY = 1024;
  static final int BATCH_SIZE = 1024;
  /**
   * Largest offset + limit tracked by a time-ordered tracker, which retains that many timestamps. Larger limits scan
   * without time-based pruning.
   */
  static final long MAX_ROWS_TRACKED = 1_000_000;

  private final OrderBy timeOrderBy;
  private final boolean descending;
  private final int rowsNeeded;

  /**
   * Size at which {@link #keys} is reduced back to {@link #rowsNeeded}.
   * <p>
   * Purposely larger than {@link #rowsNeeded} to reduce the frequency of selections, which are expensive.
   */
  private final int keysCapacity;

  /**
   * Keys of candidate timestamps, see {@link #toKey}.
   * <p>
   * A larger key is a better timestamp. Once a threshold exists, the first {@link #rowsNeeded} entries after each
   * selection are the best keys seen up to that selection.
   */
  @GuardedBy("this")
  private long[] keys;

  @GuardedBy("this")
  private int numKeys;

  @GuardedBy("this")
  private boolean hasThreshold;

  /**
   * Key of {@link #threshold}, valid when {@link #hasThreshold}.
   */
  @GuardedBy("this")
  private long thresholdKey;

  /**
   * Timestamp that at least {@link #rowsNeeded} written rows are as good or better than.
   * <p>
   * Until {@link #rowsNeeded} rows have been written, it is a value no row can be worse than.
   */
  private volatile long threshold;

  TimeOrderedScanLimitTracker(final Order order, final int rowsNeeded)
  {
    this.timeOrderBy = new OrderBy(ColumnHolder.TIME_COLUMN_NAME, order);
    this.descending = order == Order.DESCENDING;
    this.rowsNeeded = rowsNeeded;
    this.keysCapacity = rowsNeeded + Math.max(1, rowsNeeded / 2);
    this.keys = new long[Math.min(keysCapacity, INITIAL_CAPACITY)];
    this.threshold = descending ? Long.MIN_VALUE : Long.MAX_VALUE;
  }

  @VisibleForTesting
  int getRowsNeeded()
  {
    return rowsNeeded;
  }

  @Override
  public Order getTimeOrder()
  {
    return timeOrderBy.getOrder();
  }

  @Override
  public boolean isSatisfied()
  {
    // We can never definitively proclaim satisfaction because future processors may have rows that beat the threshold.
    return false;
  }

  @Override
  public boolean canSkip(final Interval interval)
  {
    // The interval can be skipped if it cannot possibly contain a row that beats or ties the threshold.
    return switch (timeOrderBy.getOrder()) {
      // The interval starts after the threshold, no row can beat it.
      case ASCENDING -> interval.getStartMillis() > threshold;
      // The interval ends before or at the threshold, no row can beat it.
      case DESCENDING -> interval.getEndMillis() <= threshold;
      case NONE -> throw DruidException.defensive("Unexpected time order[%s]", timeOrderBy.getOrder());
    };
  }

  @Override
  public RowGate forCursor(final ColumnSelectorFactory columnSelectorFactory, final List<OrderBy> cursorOrdering)
  {
    if (columnSelectorFactory.getColumnCapabilities(ColumnHolder.TIME_COLUMN_NAME) == null) {
      return RowGate.ALWAYS_WRITE;
    }

    return new RowGate()
    {
      private final BaseLongColumnValueSelector timeSelector =
          columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

      private final long[] bufferedTimestamps = new long[BATCH_SIZE];
      private int numTimestampsBuffered = 0;

      // Current row's time as read by check(), so onRowWritten() need not read the selector again.
      private boolean currentRowHasTime;
      private long currentRowTime;

      // Decision to be used when a worse than threshold row is found. If the cursor ordering matches the tracker
      // ordering, then all remaining rows will be worse than the threshold, and we can safely stop writing. If the
      // cursor ordering does not match the tracker ordering, then we can only skip this row and continue checking the
      // rest of the cursor.
      private final RowGate.Decision onWorseRow =
          !cursorOrdering.isEmpty() && cursorOrdering.getFirst().equals(timeOrderBy)
          ? RowGate.Decision.STOP
          : RowGate.Decision.SKIP;

      @Override
      public Decision check()
      {
        currentRowHasTime = !timeSelector.isNull();
        if (!currentRowHasTime) {
          // Null timestamps are not comparable with the threshold so they are always written.
          return Decision.WRITE;
        }
        currentRowTime = timeSelector.getLong();
        if (isWorse(currentRowTime)) {
          return onWorseRow;
        }
        return Decision.WRITE;
      }

      @Override
      public void onRowWritten()
      {
        // Only rows that beat the threshold can tighten it. Rows with null timestamps cannot take part at all.
        if (!currentRowHasTime || !beatsThreshold(currentRowTime)) {
          return;
        }

        bufferedTimestamps[numTimestampsBuffered++] = currentRowTime;

        if (numTimestampsBuffered == BATCH_SIZE) {
          offerAll(bufferedTimestamps, numTimestampsBuffered);
          numTimestampsBuffered = 0;
        }
      }

      @Override
      public void onFrameFlushed()
      {
        if (numTimestampsBuffered > 0) {
          offerAll(bufferedTimestamps, numTimestampsBuffered);
          numTimestampsBuffered = 0;
        }
      }
    };
  }

  /**
   * Whether a row with this {@code __time} is worse than the threshold and thus cannot contribute to the result.
   */
  boolean isWorse(final long time)
  {
    return descending ? time < threshold : time > threshold;
  }

  /**
   * Whether a row with this {@code __time} is strictly better than the threshold, and so could tighten it.
   */
  private boolean beatsThreshold(final long time)
  {
    return descending ? time > threshold : time < threshold;
  }

  /**
   * Records the {@code __time} of a written row.
   */
  @VisibleForTesting
  void offer(final long time)
  {
    offerAll(new long[]{time}, 1);
  }

  /**
   * Records the {@code __time}s of {@code count} written rows.
   */
  void offerAll(final long[] times, final int count)
  {
    synchronized (this) {
      for (int i = 0; i < count; i++) {
        final long key = toKey(times[i]);
        if (hasThreshold && key <= thresholdKey) {
          continue;
        }
        if (numKeys == keys.length) {
          keys = Arrays.copyOf(keys, Math.min(keys.length * 2, keysCapacity));
        }
        keys[numKeys++] = key;
        if (numKeys == keysCapacity) {
          keepBestKeys();
        }
      }

      if (!hasThreshold && numKeys >= rowsNeeded) {
        keepBestKeys();
      }
    }
  }

  /**
   * Reduces {@link #keys} to its best {@link #rowsNeeded} keys and publishes the worst of them as the threshold.
   */
  @GuardedBy("this")
  private void keepBestKeys()
  {
    final int start = numKeys - rowsNeeded;
    // After selection [start, numKeys) holds the best keys, with the worst of them at start (the new threshold).
    Selection.select(keys, 0, numKeys, start);
    // Move the best keys to the front of the array. New keys will stream in after them until the next selection.
    System.arraycopy(keys, start, keys, 0, rowsNeeded);
    numKeys = rowsNeeded;
    thresholdKey = keys[0];
    hasThreshold = true;
    threshold = fromKey(thresholdKey);
  }

  /**
   * Maps a timestamp to a key that is larger the better the timestamp is.
   */
  private long toKey(final long time)
  {
    // Bitwise negation reverses the order of ascending timestamps without overflow.
    return descending ? time : ~time;
  }

  /**
   * Maps a key back to its timestamp.
   */
  private long fromKey(final long key)
  {
    return descending ? key : ~key;
  }
}
