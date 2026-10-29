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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.ThrowableMatcher;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.querykit.FrameProcessorTestBase;
import org.apache.druid.msq.querykit.ReadableInput;
import org.apache.druid.msq.querykit.SegmentReferenceHolder;
import org.apache.druid.msq.test.LimitedFrameWriterFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.AsyncCursorHolder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ScanQueryFrameProcessorTest extends FrameProcessorTestBase
{

  @Test
  public void test_runWithSegments() throws Exception
  {
    final QueryableIndex queryableIndex = TestIndex.getMMappedTestIndex();

    final CursorFactory cursorFactory =
        new QueryableIndexCursorFactory(queryableIndex);

    // put funny intervals on query to ensure it is adjusted to the segment interval before building cursor
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      ImmutableList.of(
                          Intervals.of("2001-01-01T00Z/2011-01-01T00Z"),
                          Intervals.of("2011-01-02T00Z/2021-01-01T00Z")
                      )
                  )
              )
              .columns(cursorFactory.getRowSignature().getColumnNames())
              .build();

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromCursorFactory(cursorFactory, cursorFactory.getRowSignature(), false),
        Sequences.simple(runScanOverSegment(queryableIndex, query))
    );
  }

  @Test
  public void test_runWithSegments_sharedLimitReachedMidFrame() throws Exception
  {
    final QueryableIndex queryableIndex = TestIndex.getMMappedTestIndex();

    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.ETERNITY)))
              .columns("__time", "market")
              .limit(13)
              .build();

    // Other processors have already written 10 rows, so this one must stop after 3, even though a frame can hold more.
    final ScanLimitTracker limitTracker = ScanLimitTracker.create(query);
    limitTracker.forCursor(null, List.of()).onFrameFlushed();
    final List<List<Object>> rows = runScanOverSegment(queryableIndex, query, limitTracker, Integer.MAX_VALUE);

    Assertions.assertEquals(3, rows.size());
    Assertions.assertTrue(limitTracker.isSatisfied());
  }

  @Test
  public void test_runWithSegments_prunableSegment_releasedWithoutReading() throws Exception
  {
    final QueryableIndex queryableIndex = TestIndex.getMMappedTestIndex();
    final ScanQuery query = timeOrderedQuery(Order.DESCENDING);

    // Prunes every segment, but still needs rows, so the processor's early return for a satisfied tracker does not
    // apply.
    final ScanLimitTracker pruneAll = new ScanLimitTracker()
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
        return true;
      }
    };

    // runScanOverSegment also checks that the segment reference was released.
    Assertions.assertEquals(List.of(), runScanOverSegment(queryableIndex, query, pruneAll, 1));
  }

  @ParameterizedTest
  @EnumSource(value = Order.class, names = {"ASCENDING", "DESCENDING"})
  public void test_runWithSegments_timeOrderedCursor_stopsAtThreshold(final Order order) throws Exception
  {
    // Cursor returns rows in the query's __time order, so the first row worse than the threshold ends the cursor.
    final QueryableIndex queryableIndex = TestIndex.getMMappedTestIndex();
    final ScanQuery query = timeOrderedQuery(order);

    final int timeColumn = timeColumn(queryableIndex);
    final List<List<Object>> allRows = runScanOverSegment(queryableIndex, query, ScanLimitTracker.NONE, 1);
    final List<List<Object>> rows = runScanOverSegment(queryableIndex, query, trackerAtThresholdTime(order), 1);

    Assertions.assertEquals(rowsNotWorseThanThreshold(allRows, timeColumn, order), rows);
  }

  @Test
  public void test_runWithSegments_nonTimeOrderedCursor_skipsRowsPastThreshold() throws Exception
  {
    // Segment sorted by dimensions first, so rows newer than the threshold come after older ones.
    final QueryableIndex queryableIndex = TestIndex.getNonTimeOrderedMMappedTestIndex();
    final ScanQuery query = timeOrderedQuery(Order.DESCENDING);

    final int timeColumn = timeColumn(queryableIndex);
    final List<List<Object>> allRows = runScanOverSegment(queryableIndex, query, ScanLimitTracker.NONE, 1);
    final List<List<Object>> rows =
        runScanOverSegment(queryableIndex, query, trackerAtThresholdTime(Order.DESCENDING), 1);

    // A row the threshold lets through comes after one it does not, so stopping at the first worse row would lose it.
    final int firstWorseRow = Iterables.indexOf(allRows, row -> timeOf(row, timeColumn) < THRESHOLD_TIME);
    Assertions.assertTrue(firstWorseRow >= 0);
    Assertions.assertTrue(
        allRows.subList(firstWorseRow, allRows.size())
               .stream()
               .anyMatch(row -> timeOf(row, timeColumn) >= THRESHOLD_TIME)
    );

    Assertions.assertEquals(rowsNotWorseThanThreshold(allRows, timeColumn, Order.DESCENDING), rows);
  }

  @Test
  public void test_runWithSegments_descendingMatchesNonVectorized() throws Exception
  {
    final QueryableIndex queryableIndex = TestIndex.getMMappedTestIndex();

    final Druids.ScanQueryBuilder baseBuilder =
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000-01-01T00Z/2030-01-01T00Z"))))
              .columns("__time", "market", "quality", "index")
              .order(Order.DESCENDING);

    final ScanQuery nonVectorizedQuery =
        baseBuilder.context(QueryContext.ofMap(QueryContextParameters.VECTORIZE, QueryContexts.Vectorize.FALSE)).build();
    final ScanQuery vectorizedQuery =
        baseBuilder.context(
                     QueryContext.ofMap(
                         QueryContextParameters.VECTORIZE,
                         QueryContexts.Vectorize.FORCE,
                         QueryContextParameters.VECTOR_SIZE,
                         7
                     )
                 )
                   .build();

    final List<List<Object>> nonVectorizedRows = runScanOverSegment(queryableIndex, nonVectorizedQuery);
    final List<List<Object>> vectorizedRows = runScanOverSegment(queryableIndex, vectorizedQuery);

    // The vectorized descending scan must match the non-vectorized descending scan exactly.
    Assertions.assertFalse(vectorizedRows.isEmpty());
    Assertions.assertEquals(nonVectorizedRows, vectorizedRows);

    // Sanity check: __time (column 0) is non-increasing.
    long previousTime = Long.MAX_VALUE;
    for (final List<Object> row : vectorizedRows) {
      final long time = ((Number) row.getFirst()).longValue();
      Assertions.assertTrue(time <= previousTime, "descending __time");
      previousTime = time;
    }
  }

  @Test
  public void test_runWithInputChannel() throws Exception
  {
    final CursorFactory cursorFactory =
        new IncrementalIndexCursorFactory(TestIndex.getIncrementalTestIndex());

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .maxRowsPerFrame(5)
                            .frameType(FrameType.latestRowBased())
                            .allocator(ArenaMemoryAllocator.createOnHeap(100_000));

    final RowSignature signature = frameSequenceBuilder.signature();
    final List<Frame> frames = frameSequenceBuilder.frames().toList();
    final BlockingQueueFrameChannel inputChannel = new BlockingQueueFrameChannel(frames.size());
    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    try (final WritableFrameChannel writableInputChannel = inputChannel.writable()) {
      for (final Frame frame : frames) {
        writableInputChannel.write(frame);
      }
    }

    // put funny intervals on query to ensure it is validated before building cursor
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      ImmutableList.of(
                          Intervals.of("2001-01-01T00Z/2011-01-01T00Z"),
                          Intervals.of("2011-01-02T00Z/2021-01-01T00Z")
                      )
                  )
              )
              .columns(cursorFactory.getRowSignature().getColumnNames())
              .build();

    // Limit output frames to 1 row to ensure we test edge cases
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeFrameWriterFactory(
            FrameType.latestRowBased(),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            signature,
            Collections.emptyList(),
            false
        ),
        1
    );

    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        ScanLimitTracker.NONE,
        new DefaultObjectMapper(),
        ReadableInput.channel(inputChannel.readable(), FrameReader.create(signature), 0, 0),
        SegmentMapFunction.IDENTITY,
        ResourceHolder.fromCloseable(outputChannel.writable()),
        new ReferenceCountingResourceHolder<>(frameWriterFactory, () -> {})
    );

    ListenableFuture<Object> retVal = exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(signature)
    );

    final RuntimeException e = Assertions.assertThrows(
        RuntimeException.class,
        rowsFromProcessor::toList
    );

    ThrowableMatcher.of(RuntimeException.class)
                   .expectMessageContains(
                       "Expected eternity intervals, but got[[2001-01-01T00:00:00.000Z/2011-01-01T00:00:00.000Z, "
                       + "2011-01-02T00:00:00.000Z/2021-01-01T00:00:00.000Z]]"
                   )
                   .assertThat(e);
  }

  /**
   * Verifies that {@link ScanQueryFrameProcessor#runWithSegment} yields via {@link
   * org.apache.druid.frame.processor.ReturnOrAwait#awaitAllFutures} when {@link CursorFactory#makeCursorHolderAsync}
   * returns an {@link AsyncCursorHolder} that has not yet completed, and resumes after it does. Exercises the partial
   * / non-blocking I/O integration path on the MSQ side without requiring a real partial segment.
   */
  @Test
  public void test_runWithSegments_asyncCursorHolderAwaits() throws Exception
  {
    final QueryableIndex queryableIndex = TestIndex.getMMappedTestIndex();
    final CursorFactory baseCursorFactory = new QueryableIndexCursorFactory(queryableIndex);
    final AsyncCursorHolder deferredAsyncHolder = new AsyncCursorHolder(null);

    final CursorFactory deferredCursorFactory = new CursorFactory()
    {
      @Override
      public CursorHolder makeCursorHolder(CursorBuildSpec spec)
      {
        return baseCursorFactory.makeCursorHolder(spec);
      }

      @Override
      public AsyncCursorHolder makeCursorHolderAsync(CursorBuildSpec spec)
      {
        return deferredAsyncHolder;
      }

      @Override
      public RowSignature getRowSignature()
      {
        return baseCursorFactory.getRowSignature();
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return baseCursorFactory.getColumnCapabilities(column);
      }
    };

    final QueryableIndexSegment baseSegment = new QueryableIndexSegment(queryableIndex, SegmentId.dummy("test"));
    final Segment wrappedSegment = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return baseSegment.getId();
      }

      @Override
      public Interval getDataInterval()
      {
        return baseSegment.getDataInterval();
      }

      @Override
      public void validateOrElseThrow(PolicyEnforcer policyEnforcer)
      {
        baseSegment.validateOrElseThrow(policyEnforcer);
      }

      @Override
      public boolean isTombstone()
      {
        return baseSegment.isTombstone();
      }

      @Override
      public String getDebugString()
      {
        return baseSegment.getDebugString();
      }

      @SuppressWarnings("unchecked")
      @Override
      public <T> T as(@Nonnull Class<T> clazz)
      {
        if (CursorFactory.class.equals(clazz)) {
          return (T) deferredCursorFactory;
        }
        return baseSegment.as(clazz);
      }

      @Override
      public void close()
      {
        baseSegment.close();
      }
    };

    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.ETERNITY)))
              .columns(baseCursorFactory.getRowSignature().getColumnNames())
              .build();

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeFrameWriterFactory(
            FrameType.latestRowBased(),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            baseCursorFactory.getRowSignature(),
            Collections.emptyList(),
            false
        ),
        1
    );

    final ReferenceCountedSegmentProvider segmentReferenceProvider = new ReferenceCountedSegmentProvider(wrappedSegment);
    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        ScanLimitTracker.NONE,
        new DefaultObjectMapper(),
        ReadableInput.segment(
            new SegmentReferenceHolder(
                new SegmentReference(
                    SegmentId.dummy("test").toDescriptor(),
                    segmentReferenceProvider.acquireReference()
                ),
                null
            )
        ),
        SegmentMapFunction.IDENTITY,
        ResourceHolder.fromCloseable(outputChannel.writable()),
        new ReferenceCountingResourceHolder<>(frameWriterFactory, () -> {})
    );

    final ListenableFuture<Object> retVal = exec.runFully(processor, null);

    // Processor should be awaiting the deferred holder and have produced no rows yet.
    Thread.sleep(200);
    Assertions.assertFalse(retVal.isDone(), "processor should be awaiting the deferred AsyncCursorHolder");
    Assertions.assertFalse(outputChannel.readable().canRead(), "no frames should have been written yet");

    // Complete the load and verify the processor proceeds to produce all rows.
    deferredAsyncHolder.set(baseCursorFactory.makeCursorHolder(ScanQueryEngine.makeCursorBuildSpec(query, null)));

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(baseCursorFactory.getRowSignature())
    );

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromCursorFactory(baseCursorFactory, baseCursorFactory.getRowSignature(), false),
        rowsFromProcessor
    );

    Assertions.assertEquals(Unit.instance(), retVal.get(30, TimeUnit.SECONDS));
  }

  /**
   * Threshold of the time-ordered tests. Rows at this time tie it; rows after it (descending) or before it
   * (ascending) beat it.
   */
  private static final long THRESHOLD_TIME = DateTimes.of("2011-04-01").getMillis();

  /**
   * Rows needed by the tracker in the time-ordered tests: more than the test segments hold, so the rows a test writes
   * never tighten the threshold.
   */
  private static final int TRACKER_ROWS_NEEDED = 5000;

  /**
   * Tracker whose threshold is {@link #THRESHOLD_TIME}, as if other processors had already written
   * {@link #TRACKER_ROWS_NEEDED} rows at that time.
   */
  private static TimeOrderedScanLimitTracker trackerAtThresholdTime(final Order order)
  {
    final TimeOrderedScanLimitTracker limitTracker = new TimeOrderedScanLimitTracker(order, TRACKER_ROWS_NEEDED);
    final long[] times = new long[TRACKER_ROWS_NEEDED];
    Arrays.fill(times, THRESHOLD_TIME);
    limitTracker.offerAll(times, times.length);
    return limitTracker;
  }

  /**
   * Returns the rows of {@code rows} that are not worse than {@link #THRESHOLD_TIME} in {@code order}, in their
   * original order. Checks that the threshold both keeps and removes some rows, so tests using it cannot pass
   * trivially, and that the rows kept cannot tighten the threshold.
   */
  private static List<List<Object>> rowsNotWorseThanThreshold(
      final List<List<Object>> rows,
      final int timeColumn,
      final Order order
  )
  {
    final List<List<Object>> notWorse =
        rows.stream()
            .filter(row -> order == Order.DESCENDING
                           ? timeOf(row, timeColumn) >= THRESHOLD_TIME
                           : timeOf(row, timeColumn) <= THRESHOLD_TIME)
            .collect(Collectors.toList());
    Assertions.assertFalse(notWorse.isEmpty());
    Assertions.assertTrue(notWorse.size() < rows.size());
    Assertions.assertTrue(notWorse.size() < TRACKER_ROWS_NEEDED);
    return notWorse;
  }

  /**
   * Position of {@code __time} in the rows that {@link #runScanOverSegment} returns for {@code queryableIndex}.
   */
  private static int timeColumn(final QueryableIndex queryableIndex)
  {
    return new QueryableIndexCursorFactory(queryableIndex).getRowSignature().indexOf(ColumnHolder.TIME_COLUMN_NAME);
  }

  private static long timeOf(final List<Object> row, final int timeColumn)
  {
    return ((Number) row.get(timeColumn)).longValue();
  }

  /**
   * Query for the time-ordered tests: ordered by {@code __time}, with a limit larger than the test segments' rows, so
   * the limit pushdown never ends a cursor first.
   */
  private static ScanQuery timeOrderedQuery(final Order order)
  {
    return Druids.newScanQueryBuilder()
                 .dataSource("test")
                 .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.ETERNITY)))
                 .columns("__time", "market")
                 .order(order)
                 .limit(TRACKER_ROWS_NEEDED)
                 .build();
  }

  private List<List<Object>> runScanOverSegment(final QueryableIndex queryableIndex, final ScanQuery query)
      throws Exception
  {
    // Limit output frames to 1 row to ensure we test edge cases
    return runScanOverSegment(queryableIndex, query, ScanLimitTracker.NONE, 1);
  }

  private List<List<Object>> runScanOverSegment(
      final QueryableIndex queryableIndex,
      final ScanQuery query,
      final ScanLimitTracker limitTracker,
      final int maxRowsPerFrame
  ) throws Exception
  {
    final CursorFactory cursorFactory = new QueryableIndexCursorFactory(queryableIndex);
    final RowSignature signature = cursorFactory.getRowSignature();

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeFrameWriterFactory(
            FrameType.latestRowBased(),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            signature,
            Collections.emptyList(),
            false
        ),
        maxRowsPerFrame
    );

    final ReferenceCountedSegmentProvider segmentReferenceProvider =
        new ReferenceCountedSegmentProvider(new QueryableIndexSegment(queryableIndex, SegmentId.dummy("test")));
    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        limitTracker,
        new DefaultObjectMapper(),
        ReadableInput.segment(
            new SegmentReferenceHolder(
                new SegmentReference(
                    SegmentId.dummy("test").toDescriptor(),
                    segmentReferenceProvider.acquireReference()
                ),
                null
            )
        ),
        SegmentMapFunction.IDENTITY,
        ResourceHolder.fromCloseable(outputChannel.writable()),
        new ReferenceCountingResourceHolder<>(frameWriterFactory, () -> {})
    );

    final ListenableFuture<Object> retVal = exec.runFully(processor, null);

    final List<List<Object>> rows = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(signature)
    ).toList();

    Assertions.assertEquals(Unit.instance(), retVal.get());
    Assertions.assertEquals(0, segmentReferenceProvider.getNumReferences()); // Segment reference was closed
    return rows;
  }
}
