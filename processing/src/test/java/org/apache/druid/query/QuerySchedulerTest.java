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

package org.apache.druid.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.scheduling.HiLoQuerySchedulingStrategy;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class QuerySchedulerTest
{
  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Test
  public void testHiLoHi() throws ExecutionException, InterruptedException
  {
    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));

    TopNQuery interactive = makeInteractiveQuery();
    ListenableFuture<?> future = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("test_query_scheduler_%s")
    ).submit(() -> {
      try {
        Query<?> scheduled = scheduler.schedule(QueryPlus.wrap(interactive), ImmutableSet.of());

        Assert.assertNotNull(scheduled);
        Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
        Assert.assertEquals(2, scheduler.getLaneAvailableCapacity("low"));

        Sequence<Integer> underlyingSequence = makeSequence(10);
        Sequence<Integer> results = scheduler.run(scheduled, underlyingSequence);
        int rowCount = consumeAndCloseSequence(results);

        Assert.assertEquals(10, rowCount);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
    scheduler.registerQuery(interactive, future);
    future.get();
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
  }

  @Test
  public void testHiLoLo() throws ExecutionException, InterruptedException
  {
    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));
    TopNQuery report = makeReportQuery();
    ListenableFuture<?> future = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("test_query_scheduler_%s")
    ).submit(() -> {
      try {
        Query<?> scheduledReport = scheduler.schedule(QueryPlus.wrap(report), ImmutableSet.of());
        Assert.assertNotNull(scheduledReport);
        Assert.assertEquals("low", scheduledReport.getContextValue("queryLane"));
        Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
        Assert.assertEquals(1, scheduler.getLaneAvailableCapacity("low"));

        Sequence<Integer> underlyingSequence = makeSequence(10);
        Sequence<Integer> results = scheduler.run(scheduledReport, underlyingSequence);

        int rowCount = consumeAndCloseSequence(results);
        Assert.assertEquals(10, rowCount);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
    scheduler.registerQuery(report, future);
    future.get();
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(2, scheduler.getLaneAvailableCapacity("low"));
  }


  @Test
  public void testHiLoReleaseSemaphoreWhenSequenceExplodes() throws Exception
  {
    expected.expectMessage("exploded");
    expected.expect(ExecutionException.class);

    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));

    TopNQuery interactive = makeInteractiveQuery();
    ListenableFuture<?> future = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("test_query_scheduler_%s")
    ).submit(() -> {
      try {
        Query<?> scheduled = scheduler.schedule(QueryPlus.wrap(interactive), ImmutableSet.of());

        Assert.assertNotNull(scheduled);
        Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());

        Sequence<Integer> underlyingSequence = makeExplodingSequence(10);
        Sequence<Integer> results = scheduler.run(scheduled, underlyingSequence);

        consumeAndCloseSequence(results);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
    scheduler.registerQuery(interactive, future);
    future.get();
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
  }

  @Test
  public void testHiLoFailsWhenOutOfLaneCapacity()
  {
    expected.expectMessage("too many cooks");
    expected.expect(QueryCapacityExceededException.class);

    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));

    Query<?> report1 = scheduler.schedule(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of());
    Assert.assertNotNull(report1);
    Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(1, scheduler.getLaneAvailableCapacity("low"));

    Query<?> report2 = scheduler.schedule(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of());
    Assert.assertNotNull(report2);
    Assert.assertEquals(3, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(0, scheduler.getLaneAvailableCapacity("low"));

    // too many reports
    scheduler.schedule(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of());
  }

  @Test
  public void testHiLoFailsWhenOutOfTotalCapacity()
  {
    expected.expectMessage("too many cooks");
    expected.expect(QueryCapacityExceededException.class);


    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));
    Query<?> interactive1 = scheduler.schedule(QueryPlus.wrap(makeInteractiveQuery()), ImmutableSet.of());
    Assert.assertNotNull(interactive1);
    Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());

    Query<?> report1 = scheduler.schedule(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of());
    Assert.assertNotNull(report1);
    Assert.assertEquals(3, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(1, scheduler.getLaneAvailableCapacity("low"));

    Query<?> interactive2 = scheduler.schedule(QueryPlus.wrap(makeInteractiveQuery()), ImmutableSet.of());
    Assert.assertNotNull(interactive2);
    Assert.assertEquals(2, scheduler.getTotalAvailableCapacity());

    Query<?> report2 = scheduler.schedule(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of());
    Assert.assertNotNull(report2);
    Assert.assertEquals(1, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(0, scheduler.getLaneAvailableCapacity("low"));

    Query interactive3 = scheduler.schedule(QueryPlus.wrap(makeInteractiveQuery()), ImmutableSet.of());
    Assert.assertNotNull(interactive3);
    Assert.assertEquals(0, scheduler.getTotalAvailableCapacity());

    // one too many
    scheduler.schedule(QueryPlus.wrap(makeInteractiveQuery()), ImmutableSet.of());
  }


  private TopNQuery makeInteractiveQuery()
  {
    return makeBaseBuilder()
        .context(ImmutableMap.of("priority", 10, "queryId", "high-" + UUID.randomUUID()))
        .build();
  }

  private TopNQuery makeReportQuery()
  {
    return makeBaseBuilder()
        .context(ImmutableMap.of("priority", -1, "queryId", "low-" + UUID.randomUUID()))
        .build();
  }

  private TopNQueryBuilder makeBaseBuilder()
  {
    return new TopNQueryBuilder()
        .dataSource("foo")
        .intervals("2020-01-01/2020-01-02")
        .dimension("bar")
        .metric("chocula")
        .aggregators(new CountAggregatorFactory("chocula"))
        .threshold(10);
  }

  private <T> int consumeAndCloseSequence(Sequence<T> sequence) throws IOException
  {
    Yielder<T> yielder = Yielders.each(sequence);
    int rowCount = 0;
    while (!yielder.isDone()) {
      rowCount++;
      yielder = yielder.next(yielder.get());
    }
    yielder.close();
    return rowCount;
  }

  private Sequence<Integer> makeSequence(int count)
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Integer, Iterator<Integer>>()
        {
          @Override
          public Iterator<Integer> make()
          {
            return new Iterator<Integer>()
            {
              int rowCounter = 0;

              @Override
              public boolean hasNext()
              {
                return rowCounter < count;
              }

              @Override
              public Integer next()
              {
                rowCounter++;
                return rowCounter;
              }
            };
          }

          @Override
          public void cleanup(Iterator<Integer> iterFromMake)
          {
            // nothing to cleanup
          }
        }
    );
  }

  private Sequence<Integer> makeExplodingSequence(int explodeAfter)
  {
    final int explodeAt = explodeAfter + 1;
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Integer, Iterator<Integer>>()
        {
          @Override
          public Iterator<Integer> make()
          {
            return new Iterator<Integer>()
            {
              int rowCounter = 0;

              @Override
              public boolean hasNext()
              {
                return rowCounter < explodeAt;
              }

              @Override
              public Integer next()
              {
                if (rowCounter == explodeAfter) {
                  throw new RuntimeException("exploded");
                }

                rowCounter++;
                return rowCounter;
              }
            };
          }

          @Override
          public void cleanup(Iterator<Integer> iterFromMake)
          {
            // nothing to cleanup
          }
        }
    );
  }
}
