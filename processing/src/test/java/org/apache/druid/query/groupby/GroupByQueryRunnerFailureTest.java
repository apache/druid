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
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.junit.AfterClass;
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

@RunWith(Parameterized.class)
public class GroupByQueryRunnerFailureTest
{
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

    final GroupByStrategySelector strategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, BUFFER_POOL),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER,
            BUFFER_POOL
        ),
        new GroupByStrategyV2(
            DEFAULT_PROCESSING_CONFIG,
            configSupplier,
            BUFFER_POOL,
            MERGE_BUFFER_POOL,
            mapper,
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(strategySelector);
    return new GroupByQueryRunnerFactory(strategySelector, toolChest);
  }

  private static final CloseableStupidPool<ByteBuffer> BUFFER_POOL = new CloseableStupidPool<>(
      "GroupByQueryEngine-bufferPool",
      new Supplier<ByteBuffer>()
      {
        @Override
        public ByteBuffer get()
        {
          return ByteBuffer.allocateDirect(DEFAULT_PROCESSING_CONFIG.intermediateComputeSizeBytes());
        }
      }
  );
  private static final CloseableDefaultBlockingPool<ByteBuffer> MERGE_BUFFER_POOL = new CloseableDefaultBlockingPool<>(
      new Supplier<ByteBuffer>()
      {
        @Override
        public ByteBuffer get()
        {
          return ByteBuffer.allocateDirect(DEFAULT_PROCESSING_CONFIG.intermediateComputeSizeBytes());
        }
      },
      DEFAULT_PROCESSING_CONFIG.getNumMergeBuffers()
  );

  private static final GroupByQueryRunnerFactory FACTORY = makeQueryRunnerFactory(
      GroupByQueryRunnerTest.DEFAULT_MAPPER,
      new GroupByQueryConfig()
      {
        @Override
        public String getDefaultStrategy()
        {
          return "v2";
        }
      }
  );

  private QueryRunner<ResultRow> runner;

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
    for (QueryRunner<ResultRow> runner : QueryRunnerTestHelper.makeQueryRunners(FACTORY)) {
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
    expectedException.expect(QueryTimeoutException.class);
    expectedException.expectMessage("Cannot acquire enough merge buffers");

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
      expectedException.expect(QueryCapacityExceededException.class);
      expectedException.expectMessage("Cannot acquire 1 merge buffers. Try again after current running queries are finished.");
      GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);
    }
    finally {
      if (holder != null) {
        holder.forEach(ReferenceCountingResourceHolder::close);
      }
    }
  }

  @Test(timeout = 60_000L)
  public void testTimeoutExceptionOnQueryable()
  {
    expectedException.expect(QueryTimeoutException.class);

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .overrideContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1))
        .build();

    GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(
        GroupByQueryRunnerTest.DEFAULT_MAPPER,
        new GroupByQueryConfig()
        {
          @Override
          public String getDefaultStrategy()
          {
            return "v2";
          }

          @Override
          public boolean isSingleThreaded()
          {
            return true;
          }
        }
    );
    QueryRunner<ResultRow> mergeRunners = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
    GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query);
  }
}
