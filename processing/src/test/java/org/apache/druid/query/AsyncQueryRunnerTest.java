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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class AsyncQueryRunnerTest
{

  private static final long TEST_TIMEOUT_MILLIS = 60_000;
  
  private final ExecutorService executor;
  private final Query query;

  public AsyncQueryRunnerTest()
  {
    this.executor = Executors.newSingleThreadExecutor();
    query = Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .build();
  }
  
  @Test(timeout = TEST_TIMEOUT_MILLIS)
  public void testAsyncNature()
  {
    final CountDownLatch latch = new CountDownLatch(1);
    QueryRunner baseRunner = new QueryRunner()
    {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        try {
          latch.await();
          return Sequences.simple(Collections.singletonList(1));
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

    Sequence lazy = asyncRunner.run(QueryPlus.wrap(query));
    latch.countDown();
    Assert.assertEquals(Collections.singletonList(1), lazy.toList());
  }
  
  @Test(timeout = TEST_TIMEOUT_MILLIS)
  public void testQueryTimeoutHonored()
  {
    QueryRunner baseRunner = new QueryRunner()
    {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
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

    Sequence lazy =
        asyncRunner.run(QueryPlus.wrap(query.withOverriddenContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1))));

    try {
      lazy.toList();
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
    QueryRunner baseRunner = (queryPlus, responseContext) -> null;

    QueryWatcher mock = EasyMock.createMock(QueryWatcher.class);
    mock.registerQuery(EasyMock.eq(query), EasyMock.anyObject(ListenableFuture.class));
    EasyMock.replay(mock);

    AsyncQueryRunner asyncRunner = new AsyncQueryRunner<>(baseRunner, executor, mock);

    asyncRunner.run(QueryPlus.wrap(query));
    EasyMock.verify(mock);
  }
}
