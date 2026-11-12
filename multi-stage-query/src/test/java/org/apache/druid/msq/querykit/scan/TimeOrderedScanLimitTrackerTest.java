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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class TimeOrderedScanLimitTrackerTest
{
  @Test
  public void test_threshold_descending_tightensToWorstOfBestRows()
  {
    final TimeOrderedScanLimitTracker tracker = new TimeOrderedScanLimitTracker(Order.DESCENDING, 3);

    tracker.offer(10);
    tracker.offer(30);
    Assertions.assertFalse(tracker.isWorse(0), "no threshold has been set, nothing can be ruled out");
    Assertions.assertFalse(tracker.canSkip(Intervals.utc(0, 1)), "no threshold has been set, nothing can be ruled out");

    tracker.offer(20); // 10 becomes the threshold
    Assertions.assertTrue(tracker.isWorse(9));
    Assertions.assertFalse(tracker.isWorse(10), "ties can still contribute");
    Assertions.assertTrue(tracker.canSkip(Intervals.utc(0, 10)), "interval end is exclusive");
    Assertions.assertFalse(tracker.canSkip(Intervals.utc(0, 11)));

    tracker.offer(5);
    Assertions.assertFalse(tracker.isWorse(10));
    tracker.offer(40); // Threshold tightens to 20 as 10 is ejected from the best three rows.
    Assertions.assertTrue(tracker.isWorse(19));
    Assertions.assertFalse(tracker.isWorse(20));
  }

  @Test
  public void test_threshold_ascending_tightensToWorstOfBestRows()
  {
    final TimeOrderedScanLimitTracker tracker = new TimeOrderedScanLimitTracker(Order.ASCENDING, 3);

    tracker.offer(30);
    tracker.offer(10);
    Assertions.assertFalse(tracker.isWorse(Long.MAX_VALUE - 1), "no threshold has been set, nothing can be ruled out");
    Assertions.assertFalse(
        tracker.canSkip(Intervals.utc(100, 200)),
        "no threshold has been set, nothing can be ruled out"
    );

    tracker.offer(20); // Threshold becomes 30
    Assertions.assertTrue(tracker.isWorse(31));
    Assertions.assertFalse(tracker.isWorse(30), "ties can still contribute");
    Assertions.assertTrue(tracker.canSkip(Intervals.utc(31, 40)));
    Assertions.assertFalse(tracker.canSkip(Intervals.utc(30, 40)));

    tracker.offer(50);
    Assertions.assertFalse(tracker.isWorse(30));
    tracker.offer(5); // Threshold tightens to 20 as 30 is ejected from the best three rows.
    Assertions.assertTrue(tracker.isWorse(21));
    Assertions.assertFalse(tracker.isWorse(20));
  }

  @Test
  public void test_threshold_concurrentOffers_neverTighterThanTopK() throws Exception
  {
    final int rowsNeeded = 100;
    final int numThreads = 8;
    final TimeOrderedScanLimitTracker tracker = new TimeOrderedScanLimitTracker(Order.DESCENDING, rowsNeeded);

    final List<long[]> timesByThread = new ArrayList<>();
    for (int thread = 0; thread < numThreads; thread++) {
      timesByThread.add(shuffledTimesWithDuplicates(thread, numThreads, 10_000));
    }

    // Batches of 64 cross the point where the tracker fills (100 rows) partway through a batch.
    offerConcurrently(tracker, timesByThread, 64);

    // The true threshold is the 100th newest of every time offered.
    final long[] allTimes = timesByThread.stream().flatMapToLong(Arrays::stream).sorted().toArray();
    final long trueThreshold = allTimes[allTimes.length - rowsNeeded];

    Assertions.assertFalse(tracker.isWorse(trueThreshold), "never tighter than the true top 100");
    Assertions.assertTrue(tracker.isWorse(allTimes[allTimes.length - 2 * rowsNeeded]), "did tighten");
  }

  @Test
  public void test_gate_cursorInQueryTimeOrder_stopsOnWorseRow()
  {
    final TimeOrderedScanLimitTracker tracker = trackerWithThreshold(20);
    final TimeColumnSelectorFactory columnSelectorFactory = new TimeColumnSelectorFactory(true);
    final RowGate gate = tracker.forCursor(
        columnSelectorFactory,
        List.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME))
    );

    columnSelectorFactory.time = 20L;
    Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());
    columnSelectorFactory.time = 19L;
    Assertions.assertEquals(RowGate.Decision.STOP, gate.check());
  }

  @Test
  public void test_gate_cursorNotInQueryTimeOrder_skipsWorseRow()
  {
    final TimeOrderedScanLimitTracker tracker = trackerWithThreshold(20);
    final TimeColumnSelectorFactory columnSelectorFactory = new TimeColumnSelectorFactory(true);

    for (final List<OrderBy> ordering : List.of(
        List.<OrderBy>of(),
        List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        List.of(OrderBy.ascending("dim"), OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME))
    )) {
      final RowGate gate = tracker.forCursor(columnSelectorFactory, ordering);
      columnSelectorFactory.time = 19L;
      Assertions.assertEquals(RowGate.Decision.SKIP, gate.check(), ordering.toString());
      columnSelectorFactory.time = 25L;
      Assertions.assertEquals(RowGate.Decision.WRITE, gate.check(), ordering.toString());
    }
  }

  @Test
  public void test_gate_nullOrMissingTime_alwaysWrites()
  {
    final TimeOrderedScanLimitTracker tracker = new TimeOrderedScanLimitTracker(Order.ASCENDING, 1);

    Assertions.assertSame(RowGate.ALWAYS_WRITE, tracker.forCursor(new TimeColumnSelectorFactory(false), List.of()));

    // A null time read as epoch would be the best possible ascending row and set a threshold at 0.
    final TimeColumnSelectorFactory columnSelectorFactory = new TimeColumnSelectorFactory(true);
    final RowGate gate = tracker.forCursor(columnSelectorFactory, List.of());
    columnSelectorFactory.time = null;
    Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());
    gate.onRowWritten();
    gate.onFrameFlushed();
    Assertions.assertFalse(tracker.isWorse(1));
  }

  @Test
  public void test_gate_writtenRows_tightenThresholdOnFlush()
  {
    final TimeOrderedScanLimitTracker tracker = trackerWithThreshold(20);
    final TimeColumnSelectorFactory columnSelectorFactory = new TimeColumnSelectorFactory(true);
    final RowGate gate = tracker.forCursor(columnSelectorFactory, List.of());

    // Checked but not written, so these do not count.
    columnSelectorFactory.time = 40L;
    Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());
    columnSelectorFactory.time = 25L;
    Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());

    // Written rows at 30 and 40 replace both rows at the old threshold, once the gate hands them over.
    for (final long time : new long[]{30, 40}) {
      columnSelectorFactory.time = time;
      Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());
      gate.onRowWritten();
    }
    columnSelectorFactory.time = 29L;
    Assertions.assertEquals(RowGate.Decision.WRITE, gate.check(), "written rows are held until the frame flushes");

    gate.onFrameFlushed();
    Assertions.assertEquals(RowGate.Decision.SKIP, gate.check());
    columnSelectorFactory.time = 30L;
    Assertions.assertEquals(RowGate.Decision.WRITE, gate.check(), "rows that were only checked do not count");
  }

  @Test
  public void test_gate_fullBatch_handsOverBeforeFlush()
  {
    final int rowsNeeded = TimeOrderedScanLimitTracker.BATCH_SIZE;
    final TimeOrderedScanLimitTracker tracker = new TimeOrderedScanLimitTracker(Order.ASCENDING, rowsNeeded);
    final TimeColumnSelectorFactory columnSelectorFactory = new TimeColumnSelectorFactory(true);
    final RowGate gate = tracker.forCursor(columnSelectorFactory, List.of());

    for (long time = 1; time <= rowsNeeded; time++) {
      columnSelectorFactory.time = time;
      Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());
      gate.onRowWritten();
    }

    // The batch filled, so the tracker holds all rows and the threshold is the newest of them.
    columnSelectorFactory.time = (long) rowsNeeded + 1;
    Assertions.assertEquals(RowGate.Decision.SKIP, gate.check());
    columnSelectorFactory.time = (long) rowsNeeded;
    Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());

    // Two more rows in the same frame. The flush counts every row of the frame, but only these two are handed over.
    for (final long time : new long[]{500, 501}) {
      columnSelectorFactory.time = time;
      Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());
      gate.onRowWritten();
    }
    gate.onFrameFlushed();
    columnSelectorFactory.time = (long) rowsNeeded;
    Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());
  }

  /**
   * Returns a descending tracker for two rows whose threshold is {@code threshold}.
   */
  private static TimeOrderedScanLimitTracker trackerWithThreshold(final long threshold)
  {
    final TimeOrderedScanLimitTracker tracker = new TimeOrderedScanLimitTracker(Order.DESCENDING, 2);
    tracker.offer(threshold);
    tracker.offer(threshold);
    return tracker;
  }

  /**
   * Returns {@code count} distinct times owned by {@code thread}, each twice, in an order shuffled by a fixed seed.
   */
  private static long[] shuffledTimesWithDuplicates(final int thread, final int numThreads, final int count)
  {
    final List<Long> times = new ArrayList<>(2 * count);
    for (int i = 0; i < count; i++) {
      final long time = (long) i * numThreads + thread;
      times.add(time);
      times.add(time);
    }
    Collections.shuffle(times, new Random(thread));
    return times.stream().mapToLong(Long::longValue).toArray();
  }

  /**
   * Offers each array of {@code timesByThread} to {@code tracker} from its own thread, in batches of {@code batchSize},
   * and waits for all threads to finish.
   */
  private static void offerConcurrently(
      final TimeOrderedScanLimitTracker tracker,
      final List<long[]> timesByThread,
      final int batchSize
  ) throws Exception
  {
    try (ExecutorService exec = Execs.multiThreaded(timesByThread.size(), "time-ordered-scan-limit-tracker-test-%d")) {
      final List<Future<?>> futures = new ArrayList<>();
      for (final long[] times : timesByThread) {
        futures.add(exec.submit(() -> {
          for (int start = 0; start < times.length; start += batchSize) {
            final int count = Math.min(batchSize, times.length - start);
            tracker.offerAll(Arrays.copyOfRange(times, start, start + count), count);
          }
        }));
      }
      for (final Future<?> future : futures) {
        future.get();
      }
    }
  }

  /**
   * Selector factory whose only column is {@code __time}, read from {@link #time}, which may be null. Without a time
   * column, it has no columns at all.
   */
  private static class TimeColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final boolean hasTimeColumn;

    @Nullable
    private Long time;

    private TimeColumnSelectorFactory(final boolean hasTimeColumn)
    {
      this.hasTimeColumn = hasTimeColumn;
    }

    @Override
    public DimensionSelector makeDimensionSelector(final DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(final String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public long getLong()
        {
          return time == null ? 0 : time;
        }

        @Override
        public boolean isNull()
        {
          return time == null;
        }

        @Override
        public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
        {
        }
      };
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(final String column)
    {
      return hasTimeColumn && ColumnHolder.TIME_COLUMN_NAME.equals(column)
             ? ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG)
             : null;
    }
  }
}
