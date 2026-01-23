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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class GroupByQueryRunnerFailureTest
{
  private QueryProcessingPool processingPool;
  private static final DruidProcessingConfig DEFAULT_PROCESSING_CONFIG = new DruidProcessingConfig()
  {

    @Override
    public String getFormatString()
    {
      return null;
    }

    @Override
    public int intermediateComputeSizeBytes()
    {
      return 10 * 1024 * 1024;
    }

    @Override
    public int getNumMergeBuffers()
    {
      return 1;
    }

    @Override
    public int getNumThreads()
    {
      return 2;
    }
  };

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config
  )
  {
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByResourcesReservationPool groupByResourcesReservationPool =
        new GroupByResourcesReservationPool(MERGE_BUFFER_POOL, config);
    final GroupingEngine groupingEngine = new GroupingEngine(
        DEFAULT_PROCESSING_CONFIG,
        configSupplier,
        groupByResourcesReservationPool,
        TestHelper.makeJsonMapper(),
        mapper,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final GroupByQueryQueryToolChest toolChest =
        new GroupByQueryQueryToolChest(groupingEngine, groupByResourcesReservationPool);
    return new GroupByQueryRunnerFactory(groupingEngine, toolChest, BUFFER_POOL);
  }

  private static final CloseableStupidPool<ByteBuffer> BUFFER_POOL = new CloseableStupidPool<>(
      "GroupByQueryEngine-bufferPool",
      () -> ByteBuffer.allocate(DEFAULT_PROCESSING_CONFIG.intermediateComputeSizeBytes())
  );
  private static final CloseableDefaultBlockingPool<ByteBuffer> MERGE_BUFFER_POOL = new CloseableDefaultBlockingPool<>(
      () -> ByteBuffer.allocate(DEFAULT_PROCESSING_CONFIG.intermediateComputeSizeBytes()),
      DEFAULT_PROCESSING_CONFIG.getNumMergeBuffers()
  );

  private static final GroupByQueryRunnerFactory FACTORY = makeQueryRunnerFactory(
      GroupByQueryRunnerTest.DEFAULT_MAPPER,
      new GroupByQueryConfig()
      {
      }
  );

  private QueryRunner<ResultRow> runner;

  @Before
  public void setUp()
  {
    Assert.assertEquals(
        "MERGE_BUFFER_POOL size, pre-test",
        MERGE_BUFFER_POOL.maxSize(),
        MERGE_BUFFER_POOL.getPoolSize()
    );
    processingPool = new ForwardingQueryProcessingPool(
        Execs.multiThreaded(2, "GroupByQueryRunnerFailureTestExecutor-%d"),
        Execs.scheduledSingleThreaded("GroupByQueryRunnerFailureTestExecutor-Timeout-%d")
    );
  }

  @After
  public void tearDown()
  {
    Assert.assertEquals(
        "MERGE_BUFFER_POOL size, post-test",
        MERGE_BUFFER_POOL.maxSize(),
        MERGE_BUFFER_POOL.getPoolSize()
    );
    processingPool.shutdown();
  }

  @AfterClass
  public static void teardownClass()
  {
    BUFFER_POOL.close();
    MERGE_BUFFER_POOL.close();
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    final List<Object[]> args = new ArrayList<>();
    for (QueryRunner<ResultRow> runner : QueryRunnerTestHelper.makeQueryRunners(FACTORY, true)) {
      args.add(new Object[]{runner});
    }
    return args;
  }

  public GroupByQueryRunnerFailureTest(QueryRunner<ResultRow> runner)
  {
    this.runner = FACTORY.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
  }

  @Test(timeout = 60_000L)
  public void testNotEnoughMergeBuffersOnQueryable()
  {
    expectedException.expect(ResourceLimitExceededException.class);
    expectedException.expectMessage("Query needs 2 merge buffers, but only 1 merge buffers were configured");

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(
            new QueryDataSource(
                GroupByQuery.builder()
                            .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                            .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
                            .setGranularity(Granularities.ALL)
                            .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                            .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
                            .build()
            )
        )
        .setGranularity(Granularities.ALL)
        .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 500))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);
  }

  @Test(timeout = 60_000L)
  public void testResourceLimitExceededOnBroker()
  {
    expectedException.expect(ResourceLimitExceededException.class);

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(
            new QueryDataSource(
                GroupByQuery.builder()
                            .setDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                            .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(
                                                new DefaultDimensionSpec("quality", "alias"),
                                                new DefaultDimensionSpec("market", null)
                                            )
                                            .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
                                            .build()
                            )
                            .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
                            .setGranularity(Granularities.ALL)
                            .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                            .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
                            .build()
            )
        )
        .setGranularity(Granularities.ALL)
        .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 500))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);
  }

  @Test(timeout = 60_000L)
  public void testInsufficientResourcesOnBroker()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(
            new QueryDataSource(
                GroupByQuery.builder()
                            .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                            .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
                            .setGranularity(Granularities.ALL)
                            .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                            .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
                            .build()
            )
        )
        .setGranularity(Granularities.ALL)
        .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 500))
        .build();

    List<ReferenceCountingResourceHolder<ByteBuffer>> holder = null;
    try {
      holder = MERGE_BUFFER_POOL.takeBatch(1, 10);
      expectedException.expect(ResourceLimitExceededException.class);
      expectedException.expectMessage("Query needs 2 merge buffers, but only 1 merge buffers were configured");
      GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);
    }
    finally {
      if (holder != null) {
        holder.forEach(ReferenceCountingResourceHolder::close);
      }
    }
  }

  @Test(timeout = 60_000L)
  public void testTimeoutExceptionOnQueryable_singleThreaded()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1))
        .queryId("test")
        .build();

    GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(
        GroupByQueryRunnerTest.DEFAULT_MAPPER,
        new GroupByQueryConfig()
        {

          @Override
          public boolean isSingleThreaded()
          {
            return true;
          }
        }
    );
    QueryRunner<ResultRow> mockRunner = (queryPlus, responseContext) -> {
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return Sequences.empty();
    };

    QueryRunner<ResultRow> mergeRunners = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner, mockRunner));

    QueryTimeoutException ex = Assert.assertThrows(
        QueryTimeoutException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query)
    );
    // Assert overall timeout is triggered
    Assert.assertEquals("Query [test] timed out", ex.getMessage());
  }

  @Test(timeout = 60_000L)
  public void testTimeoutExceptionOnQueryable_multiThreaded()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(Map.of(QueryContexts.TIMEOUT_KEY, 1))
        .queryId("test")
        .build();

    GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(
        GroupByQueryRunnerTest.DEFAULT_MAPPER,
        new GroupByQueryConfig()
        {

          @Override
          public boolean isSingleThreaded()
          {
            return true;
          }
        }
    );
    QueryRunner<ResultRow> mockRunner = (queryPlus, responseContext) -> {
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return Sequences.empty();
    };

    QueryRunner<ResultRow> mergeRunners = factory.mergeRunners(
        Execs.directExecutor(),
        List.of(runner, mockRunner)
    );

    QueryTimeoutException ex = Assert.assertThrows(
        QueryTimeoutException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query)
    );
    // Assert overall timeout is triggered
    Assert.assertEquals("Query [test] timed out", ex.getMessage());
  }

  @Test(timeout = 20_000L)
  public void test_multiThreaded_perSegmentTimeout_causes_queryTimeout()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(Map.of(
            QueryContexts.TIMEOUT_KEY,
            300_000,
            QueryContexts.PER_SEGMENT_TIMEOUT_KEY,
            100
        ))
        .queryId("test")
        .build();

    GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(
        GroupByQueryRunnerTest.DEFAULT_MAPPER,
        new GroupByQueryConfig()
        {

          @Override
          public boolean isSingleThreaded()
          {
            return false;
          }
        }
    );
    QueryRunner<ResultRow> mockRunner = (queryPlus, responseContext) -> {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return Sequences.empty();
    };

    QueryRunner<ResultRow> mergeRunners = factory.mergeRunners(
        processingPool,
        List.of(runner, mockRunner)
    );

    QueryTimeoutException ex = Assert.assertThrows(
        QueryTimeoutException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query)
    );
    // Assert per-segment timeout is triggered
    Assert.assertEquals("Query timeout, cancelling pending results for query [test]. Per-segment timeout exceeded.", ex.getMessage());
  }

  @Test(timeout = 20_000L)
  public void test_singleThreaded_perSegmentTimeout_causes_queryTimeout()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(Map.of(
            QueryContexts.TIMEOUT_KEY,
            300_000,
            QueryContexts.PER_SEGMENT_TIMEOUT_KEY,
            100
        ))
        .queryId("test")
        .build();

    GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(
        GroupByQueryRunnerTest.DEFAULT_MAPPER,
        new GroupByQueryConfig()
        {

          @Override
          public boolean isSingleThreaded()
          {
            return true;
          }
        }
    );
    QueryRunner<ResultRow> mockRunner = (queryPlus, responseContext) -> {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return Sequences.empty();
    };

    QueryRunner<ResultRow> mergeRunners = factory.mergeRunners(
        processingPool,
        List.of(runner, mockRunner)
    );

    QueryTimeoutException ex = Assert.assertThrows(
        QueryTimeoutException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query)
    );
    // Assert per-segment timeout is triggered
    Assert.assertEquals("Query timeout, cancelling pending results for query [test]. Per-segment timeout exceeded.", ex.getMessage());
  }

  @Test(timeout = 5_000L)
  public void test_perSegmentTimeout_crossQuery() throws Exception
  {
    GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(
        GroupByQueryRunnerTest.DEFAULT_MAPPER,
        new GroupByQueryConfig()
        {
          @Override
          public boolean isSingleThreaded()
          {
            return false;
          }
        }
    );

    final GroupByQuery slowQuery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(Map.of(
            QueryContexts.TIMEOUT_KEY, 300_000,
            QueryContexts.PER_SEGMENT_TIMEOUT_KEY, 1_000
        ))
        .queryId("slow")
        .build();

    final GroupByQuery fastQuery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(Map.of(
            QueryContexts.TIMEOUT_KEY, 5_000,
            QueryContexts.PER_SEGMENT_TIMEOUT_KEY, 100
        ))
        .queryId("fast")
        .build();

    CountDownLatch slowStart = new CountDownLatch(2);
    CountDownLatch fastStart = new CountDownLatch(1);

    QueryRunner<ResultRow> signalingSlowRunner = (queryPlus, responseContext) -> {
      slowStart.countDown();
      try {
        Thread.sleep(60_000L);
      }
      catch (InterruptedException e) {
        throw new QueryInterruptedException(e);
      }
      return Sequences.empty();
    };
    QueryRunner<ResultRow> fastRunner = (queryPlus, responseContext) -> {
      fastStart.countDown();
      return Sequences.empty();
    };

    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread slowQueryThread = new Thread(() -> {
      try {
        GroupByQueryRunnerTestHelper.runQuery(
            factory,
            factory.mergeRunners(
                processingPool,
                List.of(signalingSlowRunner, signalingSlowRunner)
            ), slowQuery
        );
      }
      catch (QueryTimeoutException e) {
        thrown.set(e);
        return;
      }
      Assert.fail("Expected QueryTimeoutException for slow query");
    });

    slowQueryThread.start();
    slowStart.await();

    Thread fastQueryThread = new Thread(() -> {
      try {
        GroupByQueryRunnerTestHelper.runQuery(
            factory,
            factory.mergeRunners(
                processingPool,
                List.of(fastRunner)
            ), fastQuery
        );
      }
      catch (Exception e) {
        Assert.fail("Expected fast query to succeed");
      }
    });

    fastQueryThread.start();
    boolean fastStartedEarly = fastStart.await(500, TimeUnit.MILLISECONDS);
    Assert.assertFalse(
        "Fast query should be blocked and not started while slow queries are running",
        fastStartedEarly
    );

    fastQueryThread.join();
    slowQueryThread.join();

    Assert.assertEquals("Query timeout, cancelling pending results for query [slow]. Per-segment timeout exceeded.", thrown.get().getMessage());
  }
}
