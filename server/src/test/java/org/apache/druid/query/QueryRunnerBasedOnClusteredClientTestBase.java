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
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.com.google.common.collect.ImmutableSet;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class provides useful setup for testing {@link QueryRunner}s which work on top of
 * {@link CachingClusteredClient}. In this class, each {@link DruidServer} serves one segment
 * created using {@link SegmentGenerator}. Tests extending this class can query those segments as below:
 *
 * <pre>
 * public void test()
 * {
 *   prepareCluster(3); // prepare a cluster of 3 servers
 *   Query<T> query = makeQuery();
 *   QueryRunner<T> baseRunner = cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals());
 *   QueryRunner<T> queryRunner = makeQueryRunner(baseRunner);
 *   queryRunner.run(QueryPlus.wrap(query), responseContext())
 *   ...
 * }
 * </pre>
 */
public abstract class QueryRunnerBasedOnClusteredClientTestBase
{
  protected static final GeneratorSchemaInfo BASE_SCHEMA_INFO = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");

  private static final Closer CLOSER = Closer.create();
  private static final String DATASOURCE = "datasource";
  private static final boolean USE_PARALLEL_MERGE_POOL_CONFIGURED = false;

  protected final ObjectMapper objectMapper = new DefaultObjectMapper();
  protected final QueryToolChestWarehouse toolChestWarehouse;

  private final QueryRunnerFactoryConglomerate conglomerate;

  protected TestHttpClient httpClient;
  protected SimpleServerView simpleServerView;
  protected CachingClusteredClient cachingClusteredClient;
  protected List<DruidServer> servers;

  private SegmentGenerator segmentGenerator;

  protected QueryRunnerBasedOnClusteredClientTestBase()
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
  public static void tearDownAbstractClass() throws IOException
  {
    CLOSER.close();
  }

  @Before
  public void setupTestBase()
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
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of())
    );
    servers = new ArrayList<>();
  }

  @After
  public void tearDownTestBase() throws IOException
  {
    segmentGenerator.close();
  }

  protected void addServer(DruidServer server, DataSegment dataSegment, QueryableIndex queryableIndex)
  {
    addServer(server, dataSegment, queryableIndex, false);
  }

  protected void addServer(
      DruidServer server,
      DataSegment dataSegment,
      QueryableIndex queryableIndex,
      boolean throwQueryError
  )
  {
    servers.add(server);
    simpleServerView.addServer(server, dataSegment);
    httpClient.addServerAndRunner(
        server,
        new SimpleServerManager(conglomerate, dataSegment, queryableIndex, throwQueryError)
    );
  }

  protected void prepareCluster(int numServers)
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
          generateSegment(segment)
      );
    }
  }

  protected QueryableIndex generateSegment(DataSegment segment)
  {
    return segmentGenerator.generate(
        segment,
        new GeneratorSchemaInfo(
            BASE_SCHEMA_INFO.getColumnSchemas(),
            BASE_SCHEMA_INFO.getAggs(),
            segment.getInterval(),
            BASE_SCHEMA_INFO.isWithRollup()
        ),
        Granularities.NONE,
        10
    );
  }

  protected static Query<Result<TimeseriesResultValue>> timeseriesQuery(Interval interval)
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

  protected static List<Result<TimeseriesResultValue>> expectedTimeseriesResult(int expectedNumResultRows)
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

  protected static ResponseContext responseContext()
  {
    final ResponseContext responseContext = ConcurrentResponseContext.createEmpty();
    responseContext.put(Key.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new ConcurrentHashMap<>());
    return responseContext;
  }

  protected static DataSegment newSegment(
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
