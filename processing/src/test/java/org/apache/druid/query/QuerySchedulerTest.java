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

public class QuerySchedulerTest
{
  TopNQuery interactive = new TopNQueryBuilder()
      .dataSource("foo")
      .intervals("2020-01-01/2020-01-02")
      .dimension("bar")
      .metric("chocula")
      .aggregators(new CountAggregatorFactory("chocula"))
      .threshold(10)
      .context(ImmutableMap.of("priority", 10, "queryId", "1234"))
      .build();

  TopNQuery report = new TopNQueryBuilder()
      .dataSource("foo")
      .intervals("2020-01-01/2020-01-02")
      .dimension("bar")
      .metric("chocula")
      .aggregators(new CountAggregatorFactory("chocula"))
      .threshold(10)
      .context(ImmutableMap.of("priority", -1, "queryId", "1234"))
      .build();

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Test
  public void testHiLoHi() throws IOException
  {
    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));

    Query n2 = scheduler.schedule(QueryPlus.wrap(interactive), ImmutableSet.of());

    Assert.assertNotNull(n2);
    Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(2, scheduler.getLaneAvailableCapacity("low"));

    Sequence<Integer> exploder = makeSequence(10);
    Sequence<Integer> scheduled = scheduler.run(n2, exploder);

    int rowCount = consumeAndCloseSequence(scheduled);

    Assert.assertEquals(10, rowCount);
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
  }

  @Test
  public void testHiLoLo() throws IOException
  {
    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));

    Query q = scheduler.schedule(QueryPlus.wrap(report), ImmutableSet.of());
    Assert.assertNotNull(q);
    Assert.assertEquals("low", q.getContextValue("queryLane"));
    Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(1, scheduler.getLaneAvailableCapacity("low"));

    Sequence<Integer> exploder = makeSequence(10);
    Sequence<Integer> scheduled = scheduler.run(q, exploder);

    int rowCount = consumeAndCloseSequence(scheduled);

    Assert.assertEquals(10, rowCount);
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(2, scheduler.getLaneAvailableCapacity("low"));
  }

  @Test
  public void testHiLoFailsWhenOutOfTotalCapacity() throws IOException
  {
    expected.expectMessage("too many cooks");
    expected.expect(QueryCapacityExceededException.class);


    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));

    Query n2 = scheduler.schedule(QueryPlus.wrap(interactive), ImmutableSet.of());
    Assert.assertNotNull(n2);
    Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());

    Query n3 = scheduler.schedule(QueryPlus.wrap(report), ImmutableSet.of());
    Assert.assertNotNull(n3);
    Assert.assertEquals(3, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(1, scheduler.getLaneAvailableCapacity("low"));

    Query n4 = scheduler.schedule(QueryPlus.wrap(interactive), ImmutableSet.of());
    Assert.assertNotNull(n4);
    Assert.assertEquals(2, scheduler.getTotalAvailableCapacity());

    Query n5 = scheduler.schedule(QueryPlus.wrap(report), ImmutableSet.of());
    Assert.assertNotNull(n5);
    Assert.assertEquals(1, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(0, scheduler.getLaneAvailableCapacity("low"));

    Query n6 = scheduler.schedule(QueryPlus.wrap(interactive), ImmutableSet.of());
    Assert.assertNotNull(n6);
    Assert.assertEquals(0, scheduler.getTotalAvailableCapacity());

    scheduler.schedule(QueryPlus.wrap(interactive), ImmutableSet.of());
  }

  @Test
  public void testHiLoFailsWhenOutOfLaneCapacity()
  {
    expected.expectMessage("too many cooks");
    expected.expect(QueryCapacityExceededException.class);

    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));

    Query n2 = scheduler.schedule(QueryPlus.wrap(report), ImmutableSet.of());
    Assert.assertNotNull(n2);
    Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(1, scheduler.getLaneAvailableCapacity("low"));

    Query n3 = scheduler.schedule(QueryPlus.wrap(report), ImmutableSet.of());
    Assert.assertNotNull(n3);
    Assert.assertEquals(3, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(0, scheduler.getLaneAvailableCapacity("low"));

    scheduler.schedule(QueryPlus.wrap(report), ImmutableSet.of());
  }

  @Test
  public void testHiLoReleaseSemaphoreWhenSequenceExplodes() throws Exception
  {
    expected.expectMessage("exploded");
    expected.expect(RuntimeException.class);

    QueryScheduler scheduler = new QueryScheduler(5, new HiLoQuerySchedulingStrategy(2));

    Query n2 = scheduler.schedule(QueryPlus.wrap(interactive), ImmutableSet.of());

    Assert.assertNotNull(n2);
    Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());

    Sequence<Integer> exploder = makeExplodingSequence(10);
    Sequence<Integer> scheduled = scheduler.run(n2, exploder);

    consumeAndCloseSequence(scheduled);
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
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
