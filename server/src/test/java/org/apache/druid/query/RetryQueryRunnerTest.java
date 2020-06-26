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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
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
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RetryQueryRunnerTest
{
  private static final Closer CLOSER = Closer.create();
  private static final String DATASOURCE = "datasource";
  private static final GeneratorSchemaInfo SCHEMA_INFO = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
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
    httpClient.addServerAndRunner(server, new SimpleServerManager(conglomerate, dataSegment.getId(), queryableIndex));
  }

  @Test
  public void testNoRetry()
  {
    prepareCluster(10);
    final TimeseriesQuery query = timeseriesQuery(SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(1, false),
        query
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query));
    final List<Result<TimeseriesResultValue>> queryResult = sequence.toList();
    Assert.assertEquals(0, queryRunner.getNumTotalRetries());
    Assert.assertFalse(queryResult.isEmpty());
    Assert.assertEquals(expectedTimeseriesResult(10), queryResult);
  }

  @Test
  public void testRetryForMovedSegment()
  {
    prepareCluster(10);
    final TimeseriesQuery query = timeseriesQuery(SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(1, true),
        query
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query));

    // Let's move a segment
    dropSegmentFromServerAndAddNewServerForSegment(servers.get(0));

    final List<Result<TimeseriesResultValue>> queryResult = sequence.toList();
    Assert.assertEquals(1, queryRunner.getNumTotalRetries());
    // Note that we dropped a segment from a server, but it's still announced in the server view.
    // As a result, we may get the full result or not depending on what server will get the retry query.
    // If we hit the same server, the query will return incomplete result.
    Assert.assertTrue(queryResult.size() > 8);
    Assert.assertEquals(expectedTimeseriesResult(queryResult.size()), queryResult);
  }

  @Test
  public void testRetryUntilWeGetFullResult()
  {
    prepareCluster(10);
    final TimeseriesQuery query = timeseriesQuery(SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(100, false), // retry up to 100
        query
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query));

    // Let's move a segment
    dropSegmentFromServerAndAddNewServerForSegment(servers.get(0));

    final List<Result<TimeseriesResultValue>> queryResult = sequence.toList();
    Assert.assertTrue(0 < queryRunner.getNumTotalRetries());
    Assert.assertEquals(expectedTimeseriesResult(10), queryResult);
  }

  @Test
  public void testFailWithPartialResultsAfterRetry()
  {
    prepareCluster(10);
    final TimeseriesQuery query = timeseriesQuery(SCHEMA_INFO.getDataInterval());
    final RetryQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newRetryQueryRunnerConfig(1, false),
        query
    );
    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(QueryPlus.wrap(query));
    dropSegmentFromServer(servers.get(0));

    expectedException.expect(SegmentMissingException.class);
    expectedException.expectMessage("No results found for segments");
    try {
      sequence.toList();
    }
    finally {
      Assert.assertEquals(1, queryRunner.getNumTotalRetries());
    }
  }

  private void prepareCluster(int numServers)
  {
    for (int i = 0; i < numServers; i++) {
      final DataSegment segment = newSegment(SCHEMA_INFO.getDataInterval(), i);
      addServer(
          SimpleServerView.createServer(i + 1),
          segment,
          segmentGenerator.generate(segment, SCHEMA_INFO, Granularities.NONE, 10)
      );
    }
  }

  private Pair<SegmentId, QueryableIndex> dropSegmentFromServer(DruidServer fromServer)
  {
    final SimpleServerManager serverManager = httpClient.getServerManager(fromServer);
    Assert.assertNotNull(serverManager);
    return serverManager.dropSegment();
  }

  private void dropSegmentFromServerAndAddNewServerForSegment(DruidServer fromServer)
  {
    final Pair<SegmentId, QueryableIndex> pair = dropSegmentFromServer(fromServer);
    final DataSegment segmentToMove = fromSegmentId(pair.lhs);
    final QueryableIndex queryableIndexToMove = pair.rhs;
    addServer(
        SimpleServerView.createServer(11),
        segmentToMove,
        queryableIndexToMove
    );
  }

  private <T> RetryQueryRunner<T> createQueryRunner(RetryQueryRunnerConfig retryQueryRunnerConfig, Query<T> query)
  {
    final QueryRunner<T> baseRunner = cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals());
    return new RetryQueryRunner<>(
        baseRunner,
        cachingClusteredClient::getQueryRunnerForSegments,
        retryQueryRunnerConfig,
        objectMapper
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

  private static TimeseriesQuery timeseriesQuery(Interval interval)
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource(DATASOURCE)
                 .intervals(ImmutableList.of(interval))
                 .granularity(Granularities.DAY)
                 .aggregators(new CountAggregatorFactory("rows"))
                 .context(
                     ImmutableMap.of(
                         DirectDruidClient.QUERY_FAIL_TIME,
                         System.currentTimeMillis() + 10000
                     )
                 )
                 .build();
  }

  private static List<Result<TimeseriesResultValue>> expectedTimeseriesResult(int expectedNumResultRows)
  {
    return IntStream
        .range(0, expectedNumResultRows)
        .mapToObj(i -> new Result<>(DateTimes.of("2000-01-01"), new TimeseriesResultValue(ImmutableMap.of("rows", 10))))
        .collect(Collectors.toList());
  }

  private static DataSegment fromSegmentId(SegmentId segmentId)
  {
    return newSegment(segmentId.getInterval(), segmentId.getPartitionNum());
  }

  private static DataSegment newSegment(
      Interval interval,
      int partitionId
  )
  {
    return DataSegment.builder()
                      .dataSource(DATASOURCE)
                      .interval(interval)
                      .version("1")
                      .shardSpec(new NumberedShardSpec(partitionId, 0))
                      .size(10)
                      .build();
  }
}
