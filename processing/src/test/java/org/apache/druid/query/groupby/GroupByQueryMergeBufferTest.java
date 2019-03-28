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
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.junit.AfterClass;
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

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class GroupByQueryMergeBufferTest
{
  private static final long TIMEOUT = 5000;

  private static class TestBlockingPool extends CloseableDefaultBlockingPool<ByteBuffer>
  {
    private int minRemainBufferNum;

    public TestBlockingPool(Supplier<ByteBuffer> generator, int limit)
    {
      super(generator, limit);
      minRemainBufferNum = limit;
    }

    @Override
    public ReferenceCountingResourceHolder<ByteBuffer> take(final long timeout)
    {
      final ReferenceCountingResourceHolder<ByteBuffer> holder = super.take(timeout);
      final int poolSize = getPoolSize();
      if (minRemainBufferNum > poolSize) {
        minRemainBufferNum = poolSize;
      }
      return holder;
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

    public void resetMinRemainBufferNum()
    {
      minRemainBufferNum = PROCESSING_CONFIG.getNumMergeBuffers();
    }

    public int getMinRemainBufferNum()
    {
      return minRemainBufferNum;
    }
  }

  public static final DruidProcessingConfig PROCESSING_CONFIG = new DruidProcessingConfig()
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
            new GroupByQueryEngine(configSupplier, bufferPool),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER,
            bufferPool
        ),
        new GroupByStrategyV2(
            PROCESSING_CONFIG,
            configSupplier,
            bufferPool,
            mergeBufferPool,
            mapper,
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(
        strategySelector,
        QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()
    );
    return new GroupByQueryRunnerFactory(strategySelector, toolChest);
  }

  private static final CloseableStupidPool<ByteBuffer> bufferPool = new CloseableStupidPool<>(
      "GroupByQueryEngine-bufferPool",
      new Supplier<ByteBuffer>()
      {
        @Override
        public ByteBuffer get()
        {
          return ByteBuffer.allocateDirect(PROCESSING_CONFIG.intermediateComputeSizeBytes());
        }
      }
  );

  private static final TestBlockingPool mergeBufferPool = new TestBlockingPool(
      new Supplier<ByteBuffer>()
      {
        @Override
        public ByteBuffer get()
        {
          return ByteBuffer.allocateDirect(PROCESSING_CONFIG.intermediateComputeSizeBytes());
        }
      },
      PROCESSING_CONFIG.getNumMergeBuffers()
  );

  private static final GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(
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

  private QueryRunner<Row> runner;

  @AfterClass
  public static void teardownClass()
  {
    bufferPool.close();
    mergeBufferPool.close();
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    final List<Object[]> args = new ArrayList<>();
    for (QueryRunner<Row> runner : QueryRunnerTestHelper.makeQueryRunners(factory)) {
      args.add(new Object[]{runner});
    }
    return args;
  }

  public GroupByQueryMergeBufferTest(QueryRunner<Row> runner)
  {
    this.runner = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
  }

  @Before
  public void setup()
  {
    mergeBufferPool.resetMinRemainBufferNum();
  }

  @Test
  public void testSimpleGroupBy()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(Granularities.ALL)
        .setInterval(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, TIMEOUT))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    assertEquals(2, mergeBufferPool.getMinRemainBufferNum());
    assertEquals(3, mergeBufferPool.getPoolSize());
  }

  @Test
  public void testNestedGroupBy()
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(
            new QueryDataSource(
                GroupByQuery.builder()
                            .setDataSource(QueryRunnerTestHelper.dataSource)
                            .setInterval(QueryRunnerTestHelper.firstToThird)
                            .setGranularity(Granularities.ALL)
                            .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                            .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.rowsCount))
                            .build()
            )
        )
        .setGranularity(Granularities.ALL)
        .setInterval(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, TIMEOUT))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    assertEquals(1, mergeBufferPool.getMinRemainBufferNum());
    assertEquals(3, mergeBufferPool.getPoolSize());
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
                                            .setDataSource(QueryRunnerTestHelper.dataSource)
                                            .setInterval(QueryRunnerTestHelper.firstToThird)
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(
                                                new DefaultDimensionSpec("quality", "alias"),
                                                new DefaultDimensionSpec("market", null)
                                            )
                                            .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.rowsCount))
                                            .build()
                            )
                            .setInterval(QueryRunnerTestHelper.firstToThird)
                            .setGranularity(Granularities.ALL)
                            .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                            .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.rowsCount))
                            .build()
            )
        )
        .setGranularity(Granularities.ALL)
        .setInterval(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, TIMEOUT))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    // This should be 0 because the broker needs 2 buffers and the queryable node needs one.
    assertEquals(0, mergeBufferPool.getMinRemainBufferNum());
    assertEquals(3, mergeBufferPool.getPoolSize());
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
                                                            .setDataSource(QueryRunnerTestHelper.dataSource)
                                                            .setInterval(QueryRunnerTestHelper.firstToThird)
                                                            .setGranularity(Granularities.ALL)
                                                            .setDimensions(Lists.newArrayList(
                                                                new DefaultDimensionSpec("quality", "alias"),
                                                                new DefaultDimensionSpec("market", null),
                                                                new DefaultDimensionSpec("placement", null)
                                                            ))
                                                            .setAggregatorSpecs(Collections.singletonList(
                                                                QueryRunnerTestHelper.rowsCount))
                                                            .build()
                                            )
                                            .setInterval(QueryRunnerTestHelper.firstToThird)
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(
                                                new DefaultDimensionSpec("quality", "alias"),
                                                new DefaultDimensionSpec("market", null)
                                            )
                                            .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.rowsCount))
                                            .build()
                            )
                            .setInterval(QueryRunnerTestHelper.firstToThird)
                            .setGranularity(Granularities.ALL)
                            .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                            .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.rowsCount))
                            .build()
            )
        )
        .setGranularity(Granularities.ALL)
        .setInterval(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, TIMEOUT))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    // This should be 0 because the broker needs 2 buffers and the queryable node needs one.
    assertEquals(0, mergeBufferPool.getMinRemainBufferNum());
    assertEquals(3, mergeBufferPool.getPoolSize());
  }
}
