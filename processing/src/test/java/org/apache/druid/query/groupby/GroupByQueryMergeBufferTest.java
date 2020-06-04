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
import com.google.common.collect.Lists;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class GroupByQueryMergeBufferTest extends InitializedNullHandlingTest
{
  private static final long TIMEOUT = 5000;

  private static class TestBlockingPool extends CloseableDefaultBlockingPool<ByteBuffer>
  {
    private int minRemainBufferNum;

    TestBlockingPool(Supplier<ByteBuffer> generator, int limit)
    {
      super(generator, limit);
      minRemainBufferNum = limit;
    }

    @Override
    public List<ReferenceCountingResourceHolder<ByteBuffer>> takeBatch(final int maxElements, final long timeout)
    {
      final List<ReferenceCountingResourceHolder<ByteBuffer>> holder = super.takeBatch(maxElements, timeout);
      final int poolSize = getPoolSize();
      if (minRemainBufferNum > poolSize) {
        minRemainBufferNum = poolSize;
      }
      return holder;
    }

    void resetMinRemainBufferNum()
    {
      minRemainBufferNum = PROCESSING_CONFIG.getNumMergeBuffers();
    }

    int getMinRemainBufferNum()
    {
      return minRemainBufferNum;
    }
  }

  private static final DruidProcessingConfig PROCESSING_CONFIG = new DruidProcessingConfig()
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
      return 3;
    }

    @Override
    public int getNumThreads()
    {
      return 1;
    }
  };

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
            PROCESSING_CONFIG,
            configSupplier,
            Suppliers.ofInstance(new QueryConfig()),
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
      () -> ByteBuffer.allocateDirect(PROCESSING_CONFIG.intermediateComputeSizeBytes())
  );

  private static final TestBlockingPool MERGE_BUFFER_POOL = new TestBlockingPool(
      () -> ByteBuffer.allocateDirect(PROCESSING_CONFIG.intermediateComputeSizeBytes()),
      PROCESSING_CONFIG.getNumMergeBuffers()
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

  private final QueryRunner<ResultRow> runner;

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

  public GroupByQueryMergeBufferTest(QueryRunner<ResultRow> runner)
  {
    this.runner = FACTORY.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
  }

  @Before
  public void setup()
  {
    MERGE_BUFFER_POOL.resetMinRemainBufferNum();
  }

  @Test
  public void testSimpleGroupBy()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.ALL)
        .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, TIMEOUT))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);

    Assert.assertEquals(2, MERGE_BUFFER_POOL.getMinRemainBufferNum());
    Assert.assertEquals(3, MERGE_BUFFER_POOL.getPoolSize());
  }

  @Test
  public void testNestedGroupBy()
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
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, TIMEOUT))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);

    Assert.assertEquals(1, MERGE_BUFFER_POOL.getMinRemainBufferNum());
    Assert.assertEquals(3, MERGE_BUFFER_POOL.getPoolSize());
  }

  @Test
  public void testDoubleNestedGroupBy()
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
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, TIMEOUT))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);

    // This should be 0 because the broker needs 2 buffers and the queryable node needs one.
    Assert.assertEquals(0, MERGE_BUFFER_POOL.getMinRemainBufferNum());
    Assert.assertEquals(3, MERGE_BUFFER_POOL.getPoolSize());
  }

  @Test
  public void testTripleNestedGroupBy()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(
            new QueryDataSource(
                GroupByQuery.builder()
                            .setDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(
                                                GroupByQuery.builder()
                                                            .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                                            .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                                            .setGranularity(Granularities.ALL)
                                                            .setDimensions(Lists.newArrayList(
                                                                new DefaultDimensionSpec("quality", "alias"),
                                                                new DefaultDimensionSpec("market", null),
                                                                new DefaultDimensionSpec("placement", null)
                                                            ))
                                                            .setAggregatorSpecs(Collections.singletonList(
                                                                QueryRunnerTestHelper.ROWS_COUNT))
                                                            .build()
                                            )
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
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, TIMEOUT))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);

    // This should be 0 because the broker needs 2 buffers and the queryable node needs one.
    Assert.assertEquals(0, MERGE_BUFFER_POOL.getMinRemainBufferNum());
    Assert.assertEquals(3, MERGE_BUFFER_POOL.getPoolSize());
  }
}
