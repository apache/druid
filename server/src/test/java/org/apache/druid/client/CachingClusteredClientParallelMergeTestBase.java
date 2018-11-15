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
package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutorService;

public abstract class CachingClusteredClientParallelMergeTestBase
{
  static final String DATA_SOURCE = "test";

  private static final String VERSION = "version";
  private static final int NUM_SERVERS = 5;
  private static final int NUM_THREADS = 2;

  private final Random random = new Random(System.currentTimeMillis());

  private ExecutorService executorService;
  private CachingClusteredClient client;
  private QueryToolChestWarehouse toolChestWarehouse;

  @Before
  public void setup()
  {
    final DruidProcessingConfig processingConfig = new DruidProcessingConfig()
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
        // Need 3 buffers for CalciteQueryTest.testDoubleNestedGroupby.
        // Two buffers for the broker and one for the queryable
        return 3;
      }

      @Override
      public int getNumThreads()
      {
        return NUM_THREADS;
      }
    };

    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
            .put(
                TimeseriesQuery.class,
                new TimeseriesQueryRunnerFactory(
                    new TimeseriesQueryQueryToolChest(
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    ),
                    new TimeseriesQueryEngine(),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                TopNQuery.class,
                new TopNQueryRunnerFactory(
                    new CloseableStupidPool<>(
                        "TopNQueryRunnerFactory-bufferPool",
                        () -> ByteBuffer.allocate(10 * 1024 * 1024)
                    ),
                    new TopNQueryQueryToolChest(
                        new TopNQueryConfig(),
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    ),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                GroupByQuery.class,
                GroupByQueryRunnerTest
                    .makeQueryRunnerFactory(
                        GroupByQueryRunnerTest.DEFAULT_MAPPER,
                        new GroupByQueryConfig()
                        {
                          @Override
                          public String getDefaultStrategy()
                          {
                            return GroupByStrategySelector.STRATEGY_V2;
                          }
                        },
                        processingConfig
                    ).lhs
            )
            .build()
    );

    toolChestWarehouse = new QueryToolChestWarehouse()
    {
      @Override
      public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
      {
        return conglomerate.findFactory(query).getToolchest();
      }
    };

    final TestTimelineServerView serverView = new TestTimelineServerView();

    addServers(serverView, NUM_SERVERS, random);

    serverView.addSegmentToServer(
        createServer(1),
        createSegment("2018-01-01/2018-01-07", 0)
    );
    serverView.addSegmentToServer(
        createServer(2),
        createSegment("2018-01-01/2018-01-07", 1)
    );
    serverView.addSegmentToServer(
        createServer(3),
        createSegment("2018-01-01/2018-01-07", 2)
    );

    final ObjectMapper objectMapper = new DefaultObjectMapper();
    executorService = Execs.multiThreaded(
        processingConfig.getNumThreads(),
        "caching-clustered-client-parallel-merge-test"
    );
    client = new CachingClusteredClient(
        toolChestWarehouse,
        serverView,
        MapCache.create(1024),
        objectMapper,
        new ForegroundCachePopulator(objectMapper, new CachePopulatorStats(), 1024),
        new CacheConfig(),
        new DruidHttpClientConfig(),
        executorService,
        processingConfig
    );
  }

  @After
  public void tearDown()
  {
    executorService.shutdown();
  }

  abstract void addServers(TestTimelineServerView serverView, int numServers, Random random);

  static DruidServer createServer(int nameSuiffix)
  {
    return new DruidServer(
        "server_" + nameSuiffix,
        "127.0.0." + nameSuiffix,
        null,
        10240L,
        ServerType.HISTORICAL,
        "default",
        0
    );
  }

  private static DataSegment createSegment(String interval, int partitionNum)
  {
    return new DataSegment(
        DATA_SOURCE,
        Intervals.of(interval),
        VERSION,
        Collections.emptyMap(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("cnt", "double_met"),
        new NumberedShardSpec(partitionNum, 0),
        0,
        1024L
    );
  }

  void runAndVerify(Query expectedQuery)
  {
    final Query testQuery = expectedQuery.withOverriddenContext(
        ImmutableMap.of(QueryContexts.NUM_BROKER_PARALLEL_COMBINE_THREADS, QueryContexts.NUM_CURRENT_AVAILABLE_THREADS)
    );
    final Sequence result = runQuery(testQuery);
    final Sequence expected = runQuery(expectedQuery);

    Assert.assertEquals(expected.toList(), result.toList());
  }

  private Sequence runQuery(Query query)
  {
    final QueryRunner queryRunner = client.getQueryRunnerForIntervals(
        query,
        Collections.singletonList(Intervals.of("2018-01-01/2018-01-07"))
    );

    //noinspection unchecked
    return new FluentQueryRunnerBuilder<>(toolChestWarehouse.getToolChest(query))
        .create(queryRunner)
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration()
        .run(QueryPlus.wrap(query), new HashMap<>());
  }
}
