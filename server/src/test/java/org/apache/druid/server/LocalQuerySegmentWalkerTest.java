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

package org.apache.druid.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.scheduling.NoQueryLaningStrategy;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link LocalQuerySegmentWalker}.
 */
public class LocalQuerySegmentWalkerTest
{
  @Rule
  public QueryStackTests.Junit4ConglomerateRule conglomerateRule = new QueryStackTests.Junit4ConglomerateRule();

  private Closer closer;

  @Before
  public void setUp()
  {
    closer = Closer.create();
  }

  @After
  public void tearDown() throws IOException
  {
    closer.close();
  }

  /**
   * Regression test for https://github.com/apache/druid/issues/17163.
   *
   * Verifies that {@link QueryScheduler#cancelQuery(String)} can interrupt a query executing via
   * {@link LocalQuerySegmentWalker}. Before the fix, no future was registered with the scheduler for locally-executed
   * queries, so cancellation was a no-op and the query ran indefinitely.
   */
  @Test(timeout = 5000)
  public void testCancelQueryInterruptsExecution() throws InterruptedException
  {
    final QueryScheduler scheduler = new QueryScheduler(
        0,
        ManualQueryPrioritizationStrategy.INSTANCE,
        NoQueryLaningStrategy.INSTANCE,
        new ServerConfig()
    );

    // Latch that fires once cursor.advance() is called, signaling that the query is actively iterating.
    final CountDownLatch iterationStarted = new CountDownLatch(1);

    final SegmentWrangler blockingWrangler = (dataSource, intervals) ->
        Collections.singletonList(new BlockingSegment(iterationStarted));

    final LocalQuerySegmentWalker walker = QueryStackTests.createLocalQuerySegmentWalker(
        conglomerateRule.getConglomerate(),
        blockingWrangler,
        new org.apache.druid.segment.join.JoinableFactoryWrapper(
            new org.apache.druid.segment.join.MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of())
        ),
        scheduler,
        new StubServiceEmitter("test", "localhost")
    );

    final String queryId = "test-cancel-query";
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(InlineDataSource.fromIterable(
                                      ImmutableList.of(new Object[]{1L}),
                                      RowSignature.builder().add("x", ColumnType.LONG).build()
                                  ))
                                  .intervals(new MultipleIntervalSegmentSpec(
                                      Collections.singletonList(Intervals.ETERNITY)
                                  ))
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .context(ImmutableMap.of(org.apache.druid.query.BaseQuery.QUERY_ID, queryId))
                                  .build();

    final AtomicReference<Throwable> thrown = new AtomicReference<>();
    final Thread queryThread = new Thread(() -> {
      try {
        final QueryRunner<ScanResultValue> runner = walker.getQueryRunnerForIntervals(
            query,
            query.getIntervals()
        );
        runner.run(QueryPlus.wrap(query), org.apache.druid.query.context.ResponseContext.createEmpty()).toList();
      }
      catch (Throwable t) {
        thrown.set(t);
      }
    });
    queryThread.start();

    // Wait for the query to start iterating before cancelling.
    Assert.assertTrue("Iteration did not start in time", iterationStarted.await(2, TimeUnit.SECONDS));
    scheduler.cancelQuery(queryId);

    queryThread.join(2000);

    Assert.assertNotNull("Expected QueryInterruptedException but no exception was thrown", thrown.get());
    Assert.assertTrue(
        "Expected QueryInterruptedException but got: " + thrown.get(),
        thrown.get() instanceof QueryInterruptedException
    );
  }

  /**
   * A {@link Segment} that returns a {@link BlockingCursorFactory}.
   */
  private static class BlockingSegment implements Segment
  {
    private final CountDownLatch startSignal;

    BlockingSegment(CountDownLatch startSignal)
    {
      this.startSignal = startSignal;
    }

    @Override
    @Nullable
    public SegmentId getId()
    {
      return null;
    }

    @Override
    public Interval getDataInterval()
    {
      return Intervals.ETERNITY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T as(@Nonnull Class<T> clazz)
    {
      if (CursorFactory.class.equals(clazz)) {
        return (T) new BlockingCursorFactory(startSignal);
      }
      return null;
    }

    @Override
    public void close()
    {
    }
  }

  /**
   * A {@link CursorFactory} with an empty row signature and a cursor whose {@link Cursor#advance()} blocks
   * until the thread is interrupted, then throws {@link QueryInterruptedException}.
   */
  private static class BlockingCursorFactory implements CursorFactory
  {
    private final CountDownLatch startSignal;

    BlockingCursorFactory(CountDownLatch startSignal)
    {
      this.startSignal = startSignal;
    }

    @Override
    public CursorHolder makeCursorHolder(CursorBuildSpec spec)
    {
      final Cursor cursor = new Cursor()
      {
        @Override
        public ColumnSelectorFactory getColumnSelectorFactory()
        {
          return new AllNullColumnSelectorFactory();
        }

        @Override
        public void advance()
        {
          startSignal.countDown();
          try {
            Thread.sleep(Long.MAX_VALUE);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          BaseQuery.checkInterrupted();
        }

        @Override
        public void advanceUninterruptibly()
        {
          advance();
        }

        @Override
        public boolean isDone()
        {
          return false;
        }

        @Override
        public boolean isDoneOrInterrupted()
        {
          return Thread.currentThread().isInterrupted();
        }

        @Override
        public void reset()
        {
        }
      };

      return new CursorHolder()
      {
        @Nullable
        @Override
        public Cursor asCursor()
        {
          return cursor;
        }

        @Override
        public List<org.apache.druid.query.OrderBy> getOrdering()
        {
          return Collections.emptyList();
        }

        @Override
        public void close()
        {
        }
      };
    }

    @Override
    public RowSignature getRowSignature()
    {
      return RowSignature.empty();
    }

    @Override
    @Nullable
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  }

  /**
   * A minimal {@link ColumnSelectorFactory} that returns null/nil for everything.
   * Used by {@link BlockingCursorFactory} — never actually called for column reads because the scan
   * query has an empty {@link RowSignature} and therefore discovers zero columns.
   */
  private static class AllNullColumnSelectorFactory implements ColumnSelectorFactory
  {
    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return DimensionSelector.constant(null);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      return NilColumnValueSelector.instance();
    }

    @Override
    @Nullable
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return null;
    }
  }
}
