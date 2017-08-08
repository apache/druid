/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class AsyncQueryRunnerTest
{

  private final static long TEST_TIMEOUT = 60000;
  
  private final ExecutorService executor;
  private final Query query;

  public AsyncQueryRunnerTest()
  {
    this.executor = Executors.newSingleThreadExecutor();
    query = Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")))
              .build();
  }
  
  @Test(timeout = TEST_TIMEOUT)
  public void testAsyncNature() throws Exception
  {
    final CountDownLatch latch = new CountDownLatch(1);
    QueryRunner baseRunner = new QueryRunner()
    {
      @Override
      public Sequence run(QueryPlus queryPlus, Map responseContext)
      {
        try {
          latch.await();
          return Sequences.simple(Lists.newArrayList(1));
        }
        catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    
    AsyncQueryRunner asyncRunner = new AsyncQueryRunner<>(
        baseRunner,
        executor,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    
    Sequence lazy = asyncRunner.run(query, Collections.EMPTY_MAP);
    latch.countDown();
    Assert.assertEquals(Lists.newArrayList(1), Sequences.toList(lazy, Lists.newArrayList()));
  }
  
  @Test(timeout = TEST_TIMEOUT)
  public void testQueryTimeoutHonored()
  {
    QueryRunner baseRunner = new QueryRunner()
    {
      @Override
      public Sequence run(QueryPlus queryPlus, Map responseContext)
      {
        try {
          Thread.sleep(Long.MAX_VALUE);
          throw new RuntimeException("query should not have completed");
        }
        catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    
    AsyncQueryRunner asyncRunner = new AsyncQueryRunner<>(
        baseRunner,
        executor,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    Sequence lazy = asyncRunner.run(
        query.withOverriddenContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1)),
        Collections.EMPTY_MAP
    );

    try {
      Sequences.toList(lazy, Lists.newArrayList());
    }
    catch (RuntimeException ex) {
      Assert.assertTrue(ex.getCause() instanceof TimeoutException);
      return;
    }
    Assert.fail();
  }

  @Test
  public void testQueryRegistration()
  {
    QueryRunner baseRunner = new QueryRunner()
    {
      @Override
      public Sequence run(QueryPlus queryPlus, Map responseContext)
      {
        return null;
      }
    };

    QueryWatcher mock = EasyMock.createMock(QueryWatcher.class);
    mock.registerQuery(EasyMock.eq(query), EasyMock.anyObject(ListenableFuture.class));
    EasyMock.replay(mock);

    AsyncQueryRunner asyncRunner = new AsyncQueryRunner<>(baseRunner, executor, mock);
    
    asyncRunner.run(query, Collections.EMPTY_MAP);
    EasyMock.verify(mock);
  }
}
