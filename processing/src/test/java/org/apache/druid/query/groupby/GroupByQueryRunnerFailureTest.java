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
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@ParameterizedClass

@MethodSource("constructorFeeder")
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

  private static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config
  )
  {
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByStatsProvider groupByStatsProvider = new GroupByStatsProvider();
    final GroupByResourcesReservationPool groupByResourcesReservationPool =
        new GroupByResourcesReservationPool(MERGE_BUFFER_POOL, config);
    final GroupingEngine groupingEngine = new GroupingEngine(
        DEFAULT_PROCESSING_CONFIG,
        configSupplier,
        groupByResourcesReservationPool,
        TestHelper.makeJsonMapper(),
        mapper,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        groupByStatsProvider
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

  @BeforeEach
  public void setUp()
  {
    Assertions.assertEquals(
        MERGE_BUFFER_POOL.maxSize(),
        MERGE_BUFFER_POOL.getPoolSize(),
        "MERGE_BUFFER_POOL size, pre-test"
    );
    processingPool = new ForwardingQueryProcessingPool(
        Execs.multiThreaded(2, "GroupByQueryRunnerFailureTestExecutor-%d"),
        Execs.scheduledSingleThreaded("GroupByQueryRunnerFailureTestExecutor-Timeout-%d")
    );
  }

  @AfterEach
  public void tearDown()
  {
    Assertions.assertEquals(
        MERGE_BUFFER_POOL.maxSize(),
        MERGE_BUFFER_POOL.getPoolSize(),
        "MERGE_BUFFER_POOL size, post-test"
    );
    processingPool.shutdown();
  }

  @AfterAll
  public static void teardownClass()
  {
    BUFFER_POOL.close();
    MERGE_BUFFER_POOL.close();
  }
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

  @Test
  @org.junit.jupiter.api.Timeout(value = 60_000L, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  public void testNotEnoughMergeBuffersOnQueryable()
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
        .setContext(QueryContext.ofMap(QueryContextParameters.TIMEOUT, 500L))
        .build();

    final ResourceLimitExceededException exception = Assertions.assertThrows(
        ResourceLimitExceededException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query)
    );
    Assertions.assertTrue(
        exception.getMessage().contains("Query needs 2 merge buffers, but only 1 merge buffers were configured")
    );
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 60_000L, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  public void testResourceLimitExceededOnBroker()
  {
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
        .setContext(QueryContext.ofMap(QueryContextParameters.TIMEOUT, 500L))
        .build();

    Assertions.assertThrows(
        ResourceLimitExceededException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query)
    );
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 60_000L, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
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
        .setContext(QueryContext.ofMap(QueryContextParameters.TIMEOUT, 500L))
        .build();

    List<ReferenceCountingResourceHolder<ByteBuffer>> holder = null;
    try {
      holder = MERGE_BUFFER_POOL.takeBatch(1, 10);
      final ResourceLimitExceededException exception = Assertions.assertThrows(
          ResourceLimitExceededException.class,
          () -> GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query)
      );
      Assertions.assertTrue(
          exception.getMessage().contains("Query needs 2 merge buffers, but only 1 merge buffers were configured")
      );
    }
    finally {
      if (holder != null) {
        holder.forEach(ReferenceCountingResourceHolder::close);
      }
    }
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 60_000L, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  public void testTimeoutExceptionOnQueryable_singleThreaded()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(QueryContext.ofMap(QueryContextParameters.TIMEOUT, 1L))
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

    QueryTimeoutException ex = Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query)
    );
    // Assert overall timeout is triggered
    Assertions.assertEquals("Query [test] timed out", ex.getMessage());
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 60_000L, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  public void testTimeoutExceptionOnQueryable_multiThreaded()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(QueryContext.ofMap(QueryContextParameters.TIMEOUT, 1L))
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

    QueryTimeoutException ex = Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query)
    );
    // Assert overall timeout is triggered
    Assertions.assertEquals("Query [test] timed out", ex.getMessage());
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 20_000L, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  public void test_multiThreaded_perSegmentTimeout_causes_queryTimeout()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(QueryContext.ofMap(
            QueryContextParameters.TIMEOUT,
            300_000L,
            QueryContextParameters.PER_SEGMENT_TIMEOUT,
            100L
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

    QueryTimeoutException ex = Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query)
    );
    // Assert per-segment timeout is triggered
    Assertions.assertEquals("Query timeout, cancelling pending results for query [test]. Per-segment timeout exceeded.", ex.getMessage());
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 20_000L, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  public void test_singleThreaded_perSegmentTimeout_causes_queryTimeout()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(Granularities.ALL)
        .overrideContext(QueryContext.ofMap(
            QueryContextParameters.TIMEOUT,
            300_000L,
            QueryContextParameters.PER_SEGMENT_TIMEOUT,
            100L
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

    QueryTimeoutException ex = Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query)
    );
    // Assert per-segment timeout is triggered
    Assertions.assertEquals("Query timeout, cancelling pending results for query [test]. Per-segment timeout exceeded.", ex.getMessage());
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 5_000L, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
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
        .overrideContext(QueryContext.ofMap(
            QueryContextParameters.TIMEOUT,
            300_000L,
            QueryContextParameters.PER_SEGMENT_TIMEOUT,
            1_000L
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
        .overrideContext(QueryContext.ofMap(
            QueryContextParameters.TIMEOUT,
            5_000L,
            QueryContextParameters.PER_SEGMENT_TIMEOUT,
            100L
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
      Assertions.fail("Expected QueryTimeoutException for slow query");
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
        Assertions.fail("Expected fast query to succeed");
      }
    });

    fastQueryThread.start();
    boolean fastStartedEarly = fastStart.await(500, TimeUnit.MILLISECONDS);
    Assertions.assertFalse(
        fastStartedEarly,
        "Fast query should be blocked and not started while slow queries are running"
    );

    fastQueryThread.join();
    slowQueryThread.join();

    Assertions.assertEquals("Query timeout, cancelling pending results for query [slow]. Per-segment timeout exceeded.", thrown.get().getMessage());
  }
}
