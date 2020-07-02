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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.SimpleServerView;
import org.apache.druid.client.TestHttpClient;
import org.apache.druid.client.TestHttpClient.SimpleServerManager;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RetryQueryRunnerTest
{
  private static final Closer CLOSER = Closer.create();
  private static final String DATASOURCE = "datasource";
  private static final GeneratorSchemaInfo BASE_SCHEMA_INFO = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
  private static final boolean USE_PARALLEL_MERGE_POOL_CONFIGURED = false;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final QueryToolChestWarehouse toolChestWarehouse;
  private final QueryRunnerFactoryConglomerate conglomerate;

  private SegmentGenerator segmentGenerator;
  private TestHttpClient httpClient;
  private SimpleServerView simpleServerView;
  private CachingClusteredClient cachingClusteredClient;
  private List<DruidServer> servers;

  public RetryQueryRunnerTest()
  {
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(CLOSER, USE_PARALLEL_MERGE_POOL_CONFIGURED);

    toolChestWarehouse = new QueryToolChestWarehouse()
    {
      @Override
      public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
      {
        return conglomerate.findFactory(query).getToolchest();
      }
    };
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    CLOSER.close();
  }

  @Before
  public void setup()
  {
    segmentGenerator = new SegmentGenerator();
    httpClient = new TestHttpClient(objectMapper);
    simpleServerView = new SimpleServerView(toolChestWarehouse, objectMapper, httpClient);
    cachingClusteredClient = new CachingClusteredClient(
        toolChestWarehouse,
        simpleServerView,
        MapCache.create(0),
        objectMapper,
        new ForegroundCachePopulator(objectMapper, new CachePopulatorStats(), 0),
        new CacheConfig(),
        new DruidHttpClientConfig(),
        QueryStackTests.getProcessingConfig(USE_PARALLEL_MERGE_POOL_CONFIGURED),
        ForkJoinPool.commonPool(),
        QueryStackTests.DEFAULT_NOOP_SCHEDULER
    );
    servers = new ArrayList<>();
  }

  @After
  public void tearDown() throws IOException
  {
    segmentGenerator.close();
  }

  private void addServer(DruidServer server, DataSegment dataSegment, QueryableIndex queryableIndex)
  {
    servers.add(server);
    simpleServerView.addServer(server, dataSegment);
    httpClient.addServerAndRunner(server, new SimpleServerManager(conglomerate, dataSegment, queryableIndex));
  }

  @Test
  public void testNoRetry()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(1, false),
        query,
        () -> {}
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query), responseContext());
    final List<Result<TimeseriesResultValue>> queryResult = sequence.toList();
    Assert.assertEquals(0, queryRunner.getTotalNumRetries());
    Assert.assertFalse(queryResult.isEmpty());
    Assert.assertEquals(expectedTimeseriesResult(10), queryResult);
  }

  @Test
  public void testRetryForMovedSegment()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(1, true),
        query,
        () -> {
          // Let's move a segment
          dropSegmentFromServerAndAddNewServerForSegment(servers.get(0));
        }
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query), responseContext());

    final List<Result<TimeseriesResultValue>> queryResult = sequence.toList();
    Assert.assertEquals(1, queryRunner.getTotalNumRetries());
    // Note that we dropped a segment from a server, but it's still announced in the server view.
    // As a result, we may get the full result or not depending on what server will get the retry query.
    // If we hit the same server, the query will return incomplete result.
    Assert.assertTrue(queryResult.size() == 9 || queryResult.size() == 10);
    Assert.assertEquals(expectedTimeseriesResult(queryResult.size()), queryResult);
  }

  @Test
  public void testRetryUntilWeGetFullResult()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(100, false), // retry up to 100
        query,
        () -> {
          // Let's move a segment
          dropSegmentFromServerAndAddNewServerForSegment(servers.get(0));
        }
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query), responseContext());

    final List<Result<TimeseriesResultValue>> queryResult = sequence.toList();
    Assert.assertTrue(0 < queryRunner.getTotalNumRetries());
    Assert.assertEquals(expectedTimeseriesResult(10), queryResult);
  }

  @Test
  public void testFailWithPartialResultsAfterRetry()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(1, false),
        query,
        () -> dropSegmentFromServer(servers.get(0))
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query), responseContext());

    expectedException.expect(SegmentMissingException.class);
    expectedException.expectMessage("No results found for segments");
    try {
      sequence.toList();
    }
    finally {
      Assert.assertEquals(1, queryRunner.getTotalNumRetries());
    }
  }

  private void prepareCluster(int numServers)
  {
    Preconditions.checkArgument(numServers < 25, "Cannot be larger than 24");
    for (int i = 0; i < numServers; i++) {
      final int partitionId = i % 2;
      final int intervalIndex = i / 2;
      final Interval interval = Intervals.of("2000-01-01T%02d/PT1H", intervalIndex);
      final DataSegment segment = newSegment(interval, partitionId, 2);
      addServer(
          SimpleServerView.createServer(i + 1),
          segment,
          segmentGenerator.generate(
              segment,
              new GeneratorSchemaInfo(
                  BASE_SCHEMA_INFO.getColumnSchemas(),
                  BASE_SCHEMA_INFO.getAggs(),
                  interval,
                  BASE_SCHEMA_INFO.isWithRollup()
              ),
              Granularities.NONE,
              10
          )
      );
    }
  }

  /**
   * Drops a segment from the DruidServer. This method doesn't update the server view, but the server will stop
   * serving queries for the dropped segment.
   */
  private NonnullPair<DataSegment, QueryableIndex> dropSegmentFromServer(DruidServer fromServer)
  {
    final SimpleServerManager serverManager = httpClient.getServerManager(fromServer);
    Assert.assertNotNull(serverManager);
    return serverManager.dropSegment();
  }

  /**
   * Drops a segment from the DruidServer and update the server view.
   */
  private NonnullPair<DataSegment, QueryableIndex> unannounceSegmentFromServer(DruidServer fromServer)
  {
    final NonnullPair<DataSegment, QueryableIndex> pair = dropSegmentFromServer(fromServer);
    simpleServerView.unannounceSegmentFromServer(fromServer, pair.lhs);
    return pair;
  }

  /**
   * Drops a segment from the {@code fromServer} and creates a new server serving the dropped segment.
   * This method updates the server view.
   */
  private void dropSegmentFromServerAndAddNewServerForSegment(DruidServer fromServer)
  {
    final NonnullPair<DataSegment, QueryableIndex> pair = unannounceSegmentFromServer(fromServer);
    final DataSegment segmentToMove = pair.lhs;
    final QueryableIndex queryableIndexToMove = pair.rhs;
    addServer(
        SimpleServerView.createServer(11),
        segmentToMove,
        queryableIndexToMove
    );
  }

  private <T> RetryQueryRunner<T> createQueryRunner(
      RetryQueryRunnerConfig retryQueryRunnerConfig,
      Query<T> query,
      Runnable runnableAfterFirstAttempt
  )
  {
    final QueryRunner<T> baseRunner = cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals());
    return new RetryQueryRunner<>(
        baseRunner,
        cachingClusteredClient::getQueryRunnerForSegments,
        retryQueryRunnerConfig,
        objectMapper,
        runnableAfterFirstAttempt
    );
  }

  private static RetryQueryRunnerConfig newRetryQueryRunnerConfig(int numTries, boolean returnPartialResults)
  {
    return new RetryQueryRunnerConfig()
    {
      @Override
      public int getNumTries()
      {
        return numTries;
      }

      @Override
      public boolean isReturnPartialResults()
      {
        return returnPartialResults;
      }
    };
  }

  private static Query<Result<TimeseriesResultValue>> timeseriesQuery(Interval interval)
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource(DATASOURCE)
                 .intervals(ImmutableList.of(interval))
                 .granularity(Granularities.HOUR)
                 .aggregators(new CountAggregatorFactory("rows"))
                 .context(
                     ImmutableMap.of(
                         DirectDruidClient.QUERY_FAIL_TIME,
                         System.currentTimeMillis() + 10000
                     )
                 )
                 .build()
                 .withId(UUID.randomUUID().toString());
  }

  private ResponseContext responseContext()
  {
    final ResponseContext responseContext = ConcurrentResponseContext.createEmpty();
    responseContext.put(Key.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new ConcurrentHashMap<>());
    return responseContext;
  }

  private static List<Result<TimeseriesResultValue>> expectedTimeseriesResult(int expectedNumResultRows)
  {
    return IntStream
        .range(0, expectedNumResultRows)
        .mapToObj(
            i -> new Result<>(
                DateTimes.of(StringUtils.format("2000-01-01T%02d", i / 2)),
                new TimeseriesResultValue(ImmutableMap.of("rows", 10))
            )
        )
        .collect(Collectors.toList());
  }

  private static DataSegment newSegment(
      Interval interval,
      int partitionId,
      int numCorePartitions
  )
  {
    return DataSegment.builder()
                      .dataSource(DATASOURCE)
                      .interval(interval)
                      .version("1")
                      .shardSpec(new NumberedShardSpec(partitionId, numCorePartitions))
                      .size(10)
                      .build();
  }
}
