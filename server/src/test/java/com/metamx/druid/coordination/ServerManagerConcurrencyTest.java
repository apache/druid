/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.coordination;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingSequenceBase;
import com.metamx.druid.Druids;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.index.ReferenceCountingSegment;
import com.metamx.druid.index.Segment;
import com.metamx.druid.metrics.NoopServiceEmitter;
import com.metamx.druid.query.NoopQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 */
public class ServerManagerConcurrencyTest
{
  private TestServerManager serverManager;
  private ConcurrencyTestQueryRunnerFactory factory;
  private CountDownLatch queryWaitLatch;
  private CountDownLatch queryNotifyLatch;

  @Before
  public void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    queryWaitLatch = new CountDownLatch(1);
    queryNotifyLatch = new CountDownLatch(1);
    factory = new ConcurrencyTestQueryRunnerFactory(queryWaitLatch, queryNotifyLatch);
    serverManager = new TestServerManager(factory);

    serverManager.loadQueryable("test", "1", new Interval("P1d/2011-04-01"));
    serverManager.loadQueryable("test", "1", new Interval("P1d/2011-04-02"));
    serverManager.loadQueryable("test", "2", new Interval("P1d/2011-04-02"));
    serverManager.loadQueryable("test", "1", new Interval("P1d/2011-04-03"));
    serverManager.loadQueryable("test", "1", new Interval("P1d/2011-04-04"));
    serverManager.loadQueryable("test", "1", new Interval("P1d/2011-04-05"));
    serverManager.loadQueryable("test", "2", new Interval("PT1h/2011-04-04T01"));
    serverManager.loadQueryable("test", "2", new Interval("PT1h/2011-04-04T02"));
    serverManager.loadQueryable("test", "2", new Interval("PT1h/2011-04-04T03"));
    serverManager.loadQueryable("test", "2", new Interval("PT1h/2011-04-04T05"));
    serverManager.loadQueryable("test", "2", new Interval("PT1h/2011-04-04T06"));
    serverManager.loadQueryable("test2", "1", new Interval("P1d/2011-04-01"));
    serverManager.loadQueryable("test2", "1", new Interval("P1d/2011-04-02"));
  }

  @Test
  public void testReferenceCounting() throws Exception
  {
    serverManager.loadQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        QueryGranularity.DAY,
        "test", new Interval("2011-04-04/2011-04-06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("3", new Interval("2011-04-04/2011-04-05"))
        )
    );

    queryNotifyLatch.await();

    Assert.assertTrue(factory.getAdapters().size() == 1);

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertFalse(segmentForTesting.isClosed());
    }

    queryWaitLatch.countDown();
    future.get();

    serverManager.dropQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertTrue(segmentForTesting.isClosed());
    }
  }

  @Test
  public void testReferenceCountingWhileQueryExecuting() throws Exception
  {
    serverManager.loadQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        QueryGranularity.DAY,
        "test", new Interval("2011-04-04/2011-04-06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("3", new Interval("2011-04-04/2011-04-05"))
        )
    );

    queryNotifyLatch.await();

    Assert.assertTrue(factory.getAdapters().size() == 1);

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertFalse(segmentForTesting.isClosed());
    }

    serverManager.dropQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertFalse(segmentForTesting.isClosed());
    }

    queryWaitLatch.countDown();
    future.get();

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertTrue(segmentForTesting.isClosed());
    }
  }

  private <T> Future assertQueryable(
      QueryGranularity granularity,
      String dataSource,
      Interval interval,
      List<Pair<String, Interval>> expected
  )
  {
    final Iterator<Pair<String, Interval>> expectedIter = expected.iterator();
    final List<Interval> intervals = Arrays.asList(interval);
    final SearchQuery query = Druids.newSearchQueryBuilder()
                                    .dataSource(dataSource)
                                    .intervals(intervals)
                                    .granularity(granularity)
                                    .limit(10000)
                                    .query("wow")
                                    .build();
    final QueryRunner<Result<SearchResultValue>> runner = serverManager.getQueryRunnerForIntervals(
        query,
        intervals
    );

    return Executors.newSingleThreadExecutor().submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            Sequence<Result<SearchResultValue>> seq = runner.run(query);
            Sequences.toList(seq, Lists.<Result<SearchResultValue>>newArrayList());
            Iterator<SegmentForTesting> adaptersIter = factory.getAdapters().iterator();

            while (expectedIter.hasNext() && adaptersIter.hasNext()) {
              Pair<String, Interval> expectedVals = expectedIter.next();
              SegmentForTesting value = adaptersIter.next();

              Assert.assertEquals(expectedVals.lhs, value.getVersion());
              Assert.assertEquals(expectedVals.rhs, value.getInterval());
            }

            Assert.assertFalse(expectedIter.hasNext());
            Assert.assertFalse(adaptersIter.hasNext());
          }
        }
    );
  }

  public static class ConcurrencyTestQueryRunnerFactory extends ServerManagerTest.MyQueryRunnerFactory
  {
    private final CountDownLatch waitLatch;
    private final CountDownLatch notifyLatch;

    private List<SegmentForTesting> adapters = Lists.newArrayList();

    public ConcurrencyTestQueryRunnerFactory(CountDownLatch waitLatch, CountDownLatch notifyLatch)
    {
      this.waitLatch = waitLatch;
      this.notifyLatch = notifyLatch;
    }

    @Override
    public QueryRunner<Result<SearchResultValue>> createRunner(Segment adapter)
    {
      if (!(adapter instanceof ReferenceCountingSegment)) {
        throw new IAE("Expected instance of ReferenceCountingSegment, got %s", adapter.getClass());
      }
      adapters.add((SegmentForTesting) ((ReferenceCountingSegment) adapter).getBaseSegment());
      return new BlockingQueryRunner<Result<SearchResultValue>>(
          new NoopQueryRunner<Result<SearchResultValue>>(),
          waitLatch,
          notifyLatch
      );
    }

    @Override
    public List<SegmentForTesting> getAdapters()
    {
      return adapters;
    }

    @Override
    public void clearAdapters()
    {
      adapters.clear();
    }
  }

  private static class BlockingQueryRunner<T> implements QueryRunner<T>
  {
    private final QueryRunner<T> runner;
    private final CountDownLatch waitLatch;
    private final CountDownLatch notifyLatch;

    public BlockingQueryRunner(
        QueryRunner<T> runner,
        CountDownLatch waitLatch,
        CountDownLatch notifyLatch
    )
    {
      this.runner = runner;
      this.waitLatch = waitLatch;
      this.notifyLatch = notifyLatch;
    }

    @Override
    public Sequence<T> run(Query<T> query)
    {
      return new BlockingSequence<T>(runner.run(query), waitLatch, notifyLatch);
    }
  }

  private static class BlockingSequence<T> extends YieldingSequenceBase<T>
  {
    private final Sequence<T> baseSequence;
    private final CountDownLatch waitLatch;
    private final CountDownLatch notifyLatch;

    public BlockingSequence(
        Sequence<T> baseSequence,
        CountDownLatch waitLatch,
        CountDownLatch notifyLatch
    )
    {
      this.baseSequence = baseSequence;
      this.waitLatch = waitLatch;
      this.notifyLatch = notifyLatch;
    }

    @Override
    public <OutType> Yielder<OutType> toYielder(
        final OutType initValue, final YieldingAccumulator<OutType, T> accumulator
    )
    {
      notifyLatch.countDown();

      final Yielder<OutType> baseYielder = baseSequence.toYielder(initValue, accumulator);
      return new Yielder<OutType>()
      {
        @Override
        public OutType get()
        {
          try {
            waitLatch.await();
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
          return baseYielder.get();
        }

        @Override
        public Yielder<OutType> next(OutType initValue)
        {
          return baseYielder.next(initValue);
        }

        @Override
        public boolean isDone()
        {
          return baseYielder.isDone();
        }

        @Override
        public void close() throws IOException
        {
          baseYielder.close();
        }
      };
    }
  }
}