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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.client.cache.BackgroundCachePopulator;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.MergeIterable;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.nary.TrinaryFn;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.search.SearchHit;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchQueryConfig;
import org.apache.druid.query.search.SearchQueryQueryToolChest;
import org.apache.druid.query.search.SearchResultValue;
import org.apache.druid.query.select.EventHolder;
import org.apache.druid.query.select.PagingSpec;
import org.apache.druid.query.select.SelectQuery;
import org.apache.druid.query.select.SelectQueryConfig;
import org.apache.druid.query.select.SelectQueryQueryToolChest;
import org.apache.druid.query.select.SelectResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryResultValue;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.apache.druid.timeline.partition.SingleElementPartitionChunk;
import org.apache.druid.timeline.partition.StringPartitionChunk;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;

/**
 *
 */
@RunWith(Parameterized.class)
public class CachingClusteredClientTest
{
  private static final ImmutableMap<String, Object> CONTEXT = ImmutableMap.of(
      "finalize", false,

      // GroupBy v2 won't cache on the broker, so test with v1.
      "groupByStrategy", GroupByStrategySelector.STRATEGY_V1
  );
  private static final MultipleIntervalSegmentSpec SEG_SPEC = new MultipleIntervalSegmentSpec(ImmutableList.of());
  private static final String DATA_SOURCE = "test";
  private static final ObjectMapper JSON_MAPPER = CachingClusteredClientTestUtils.createObjectMapper();

  /**
   * We want a deterministic test, but we'd also like a bit of randomness for the distribution of segments
   * across servers.  Thus, we loop multiple times and each time use a deterministically created Random instance.
   * Increase this value to increase exposure to random situations at the expense of test run time.
   */
  private static final int RANDOMNESS = 10;
  private static final List<AggregatorFactory> AGGS = Arrays.asList(
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("imps", "imps"),
      new LongSumAggregatorFactory("impers", "imps")
  );
  private static final List<PostAggregator> POST_AGGS = Arrays.asList(
      new ArithmeticPostAggregator(
          "avg_imps_per_row",
          "/",
          Arrays.asList(
              new FieldAccessPostAggregator("imps", "imps"),
              new FieldAccessPostAggregator("rows", "rows")
          )
      ),
      new ArithmeticPostAggregator(
          "avg_imps_per_row_double",
          "*",
          Arrays.asList(
              new FieldAccessPostAggregator("avg_imps_per_row", "avg_imps_per_row"),
              new ConstantPostAggregator("constant", 2)
          )
      ),
      new ArithmeticPostAggregator(
          "avg_imps_per_row_half",
          "/",
          Arrays.asList(
              new FieldAccessPostAggregator("avg_imps_per_row", "avg_imps_per_row"),
              new ConstantPostAggregator("constant", 2)
          )
      )
  );
  private static final List<AggregatorFactory> RENAMED_AGGS = Arrays.asList(
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("imps", "imps"),
      new LongSumAggregatorFactory("impers2", "imps")
  );
  private static final List<PostAggregator> DIFF_ORDER_POST_AGGS = Arrays.asList(
      new ArithmeticPostAggregator(
          "avg_imps_per_row",
          "/",
          Arrays.asList(
              new FieldAccessPostAggregator("imps", "imps"),
              new FieldAccessPostAggregator("rows", "rows")
          )
      ),
      new ArithmeticPostAggregator(
          "avg_imps_per_row_half",
          "/",
          Arrays.asList(
              new FieldAccessPostAggregator("avg_imps_per_row", "avg_imps_per_row"),
              new ConstantPostAggregator("constant", 2)
          )
      ),
      new ArithmeticPostAggregator(
          "avg_imps_per_row_double",
          "*",
          Arrays.asList(
              new FieldAccessPostAggregator("avg_imps_per_row", "avg_imps_per_row"),
              new ConstantPostAggregator("constant", 2)
          )
      )
  );
  private static final DimFilter DIM_FILTER = null;
  private static final List<PostAggregator> RENAMED_POST_AGGS = ImmutableList.of();
  private static final Granularity GRANULARITY = Granularities.DAY;
  private static final DateTimeZone TIMEZONE = DateTimes.inferTzFromString("America/Los_Angeles");
  private static final Granularity PT1H_TZ_GRANULARITY = new PeriodGranularity(new Period("PT1H"), null, TIMEZONE);
  private static final String TOP_DIM = "a_dim";
  private static final Supplier<SelectQueryConfig> SELECT_CONFIG_SUPPLIER = Suppliers.ofInstance(
      new SelectQueryConfig(true)
  );
  private static final Pair<QueryToolChestWarehouse, Closer> WAREHOUSE_AND_CLOSER = CachingClusteredClientTestUtils
      .createWarehouse(JSON_MAPPER, SELECT_CONFIG_SUPPLIER);
  private static final QueryToolChestWarehouse WAREHOUSE = WAREHOUSE_AND_CLOSER.lhs;
  private static final Closer RESOURCE_CLOSER = WAREHOUSE_AND_CLOSER.rhs;

  private final Random random;

  private CachingClusteredClient client;
  private Runnable queryCompletedCallback;
  private TimelineServerView serverView;
  private VersionedIntervalTimeline<String, ServerSelector> timeline;
  private Cache cache;
  private DruidServer[] servers;

  public CachingClusteredClientTest(int randomSeed)
  {
    this.random = new Random(randomSeed);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return Lists.transform(
        Lists.newArrayList(new RangeIterable(RANDOMNESS)),
        new Function<Integer, Object[]>()
        {
          @Override
          public Object[] apply(Integer input)
          {
            return new Object[]{input};
          }
        }
    );
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    RESOURCE_CLOSER.close();
  }

  @Before
  public void setUp()
  {
    timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    serverView = EasyMock.createNiceMock(TimelineServerView.class);
    cache = MapCache.create(100000);
    client = makeClient(new ForegroundCachePopulator(JSON_MAPPER, new CachePopulatorStats(), -1));

    servers = new DruidServer[]{
        new DruidServer("test1", "test1", null, 10, ServerType.HISTORICAL, "bye", 0),
        new DruidServer("test2", "test2", null, 10, ServerType.HISTORICAL, "bye", 0),
        new DruidServer("test3", "test3", null, 10, ServerType.HISTORICAL, "bye", 0),
        new DruidServer("test4", "test4", null, 10, ServerType.HISTORICAL, "bye", 0),
        new DruidServer("test5", "test5", null, 10, ServerType.HISTORICAL, "bye", 0)
    };
  }

  @Test
  public void testOutOfOrderBackgroundCachePopulation()
  {
    // This test is a bit whacky, but I couldn't find a better way to do it in the current framework.

    // The purpose of this special executor is to randomize execution of tasks on purpose.
    // Since we don't know the number of tasks to be executed, a special DrainTask is used
    // to trigger the actual execution when we are ready to shuffle the order.
    abstract class DrainTask implements Runnable
    {
    }
    final ForwardingListeningExecutorService randomizingExecutorService = new ForwardingListeningExecutorService()
    {
      final ConcurrentLinkedDeque<Pair<SettableFuture, Object>> taskQueue = new ConcurrentLinkedDeque<>();
      final ListeningExecutorService delegate = MoreExecutors.listeningDecorator(
          // we need to run everything in the same thread to ensure all callbacks on futures in CachingClusteredClient
          // are complete before moving on to the next query run.
          Execs.directExecutor()
      );

      @Override
      protected ListeningExecutorService delegate()
      {
        return delegate;
      }

      private <T> ListenableFuture<T> maybeSubmitTask(Object task, boolean wait)
      {
        if (wait) {
          SettableFuture<T> future = SettableFuture.create();
          taskQueue.addFirst(Pair.of(future, task));
          return future;
        } else {
          List<Pair<SettableFuture, Object>> tasks = Lists.newArrayList(taskQueue.iterator());
          Collections.shuffle(tasks, new Random(0));

          for (final Pair<SettableFuture, Object> pair : tasks) {
            ListenableFuture future = pair.rhs instanceof Callable ?
                                      delegate.submit((Callable) pair.rhs) :
                                      delegate.submit((Runnable) pair.rhs);
            Futures.addCallback(
                future,
                new FutureCallback()
                {
                  @Override
                  public void onSuccess(@Nullable Object result)
                  {
                    pair.lhs.set(result);
                  }

                  @Override
                  public void onFailure(Throwable t)
                  {
                    pair.lhs.setException(t);
                  }
                }
            );
          }
        }
        return task instanceof Callable ?
               delegate.submit((Callable) task) :
               (ListenableFuture<T>) delegate.submit((Runnable) task);
      }

      @SuppressWarnings("ParameterPackage")
      @Override
      public <T> ListenableFuture<T> submit(Callable<T> task)
      {
        return maybeSubmitTask(task, true);
      }

      @Override
      public ListenableFuture<?> submit(Runnable task)
      {
        if (task instanceof DrainTask) {
          return maybeSubmitTask(task, false);
        } else {
          return maybeSubmitTask(task, true);
        }
      }
    };

    client = makeClient(
        new BackgroundCachePopulator(
            randomizingExecutorService,
            JSON_MAPPER,
            new CachePopulatorStats(),
            -1
        )
    );

    // callback to be run every time a query run is complete, to ensure all background
    // caching tasks are executed, and cache is populated before we move onto the next query
    queryCompletedCallback = new Runnable()
    {
      @Override
      public void run()
      {
        try {
          randomizingExecutorService.submit(
              new DrainTask()
              {
                @Override
                public void run()
                {
                  // no-op
                }
              }
          ).get();
        }
        catch (Exception e) {
          Throwables.propagate(e);
        }
      }
    };

    final Druids.TimeseriesQueryBuilder builder = Druids.newTimeseriesQueryBuilder()
                                                        .dataSource(DATA_SOURCE)
                                                        .intervals(SEG_SPEC)
                                                        .filters(DIM_FILTER)
                                                        .granularity(GRANULARITY)
                                                        .aggregators(AGGS)
                                                        .postAggregators(POST_AGGS)
                                                        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(), new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    testQueryCaching(
        runner,
        builder.build(),
        Intervals.of("2011-01-05/2011-01-10"),
        makeTimeResults(
            DateTimes.of("2011-01-05"), 85, 102,
            DateTimes.of("2011-01-06"), 412, 521,
            DateTimes.of("2011-01-07"), 122, 21894,
            DateTimes.of("2011-01-08"), 5, 20,
            DateTimes.of("2011-01-09"), 18, 521
        ),
        Intervals.of("2011-01-10/2011-01-13"),
        makeTimeResults(
            DateTimes.of("2011-01-10"), 85, 102,
            DateTimes.of("2011-01-11"), 412, 521,
            DateTimes.of("2011-01-12"), 122, 21894
        )
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeseriesCaching()
  {
    final Druids.TimeseriesQueryBuilder builder = Druids.newTimeseriesQueryBuilder()
                                                        .dataSource(DATA_SOURCE)
                                                        .intervals(SEG_SPEC)
                                                        .filters(DIM_FILTER)
                                                        .granularity(GRANULARITY)
                                                        .aggregators(AGGS)
                                                        .postAggregators(POST_AGGS)
                                                        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(), new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    testQueryCaching(
        runner,
        builder.build(),
        Intervals.of("2011-01-01/2011-01-02"), makeTimeResults(DateTimes.of("2011-01-01"), 50, 5000),
        Intervals.of("2011-01-02/2011-01-03"), makeTimeResults(DateTimes.of("2011-01-02"), 30, 6000),
        Intervals.of("2011-01-04/2011-01-05"), makeTimeResults(DateTimes.of("2011-01-04"), 23, 85312),

        Intervals.of("2011-01-05/2011-01-10"),
        makeTimeResults(
            DateTimes.of("2011-01-05"), 85, 102,
            DateTimes.of("2011-01-06"), 412, 521,
            DateTimes.of("2011-01-07"), 122, 21894,
            DateTimes.of("2011-01-08"), 5, 20,
            DateTimes.of("2011-01-09"), 18, 521
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeTimeResults(
            DateTimes.of("2011-01-05T01"), 80, 100,
            DateTimes.of("2011-01-06T01"), 420, 520,
            DateTimes.of("2011-01-07T01"), 12, 2194,
            DateTimes.of("2011-01-08T01"), 59, 201,
            DateTimes.of("2011-01-09T01"), 181, 52
        )
    );


    HashMap<String, List> context = new HashMap<String, List>();
    TimeseriesQuery query = builder.intervals("2011-01-01/2011-01-10")
                                   .aggregators(RENAMED_AGGS)
                                   .postAggregators(RENAMED_POST_AGGS)
                                   .build();
    TestHelper.assertExpectedResults(
        makeRenamedTimeResults(
            DateTimes.of("2011-01-01"), 50, 5000,
            DateTimes.of("2011-01-02"), 30, 6000,
            DateTimes.of("2011-01-04"), 23, 85312,
            DateTimes.of("2011-01-05"), 85, 102,
            DateTimes.of("2011-01-05T01"), 80, 100,
            DateTimes.of("2011-01-06"), 412, 521,
            DateTimes.of("2011-01-06T01"), 420, 520,
            DateTimes.of("2011-01-07"), 122, 21894,
            DateTimes.of("2011-01-07T01"), 12, 2194,
            DateTimes.of("2011-01-08"), 5, 20,
            DateTimes.of("2011-01-08T01"), 59, 201,
            DateTimes.of("2011-01-09"), 18, 521,
            DateTimes.of("2011-01-09T01"), 181, 52
        ),
        runner.run(QueryPlus.wrap(query), context)
    );
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testCachingOverBulkLimitEnforcesLimit()
  {
    final int limit = 10;
    final Interval interval = Intervals.of("2011-01-01/2011-01-02");
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATA_SOURCE)
                                        .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(interval)))
                                        .filters(DIM_FILTER)
                                        .granularity(GRANULARITY)
                                        .aggregators(AGGS)
                                        .postAggregators(POST_AGGS)
                                        .context(CONTEXT)
                                        .build();

    final Map<String, Object> context = new HashMap<>();
    final Cache cache = EasyMock.createStrictMock(Cache.class);
    final Capture<Iterable<Cache.NamedKey>> cacheKeyCapture = EasyMock.newCapture();
    EasyMock.expect(cache.getBulk(EasyMock.capture(cacheKeyCapture)))
            .andReturn(ImmutableMap.of())
            .once();
    EasyMock.replay(cache);
    client = makeClient(new ForegroundCachePopulator(JSON_MAPPER, new CachePopulatorStats(), -1), cache, limit);
    final DruidServer lastServer = servers[random.nextInt(servers.length)];
    final DataSegment dataSegment = EasyMock.createNiceMock(DataSegment.class);
    EasyMock.expect(dataSegment.getId()).andReturn(SegmentId.dummy(DATA_SOURCE)).anyTimes();
    EasyMock.replay(dataSegment);
    final ServerSelector selector = new ServerSelector(
        dataSegment,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
    );
    selector.addServerAndUpdateSegment(new QueryableDruidServer(lastServer, null), dataSegment);
    timeline.add(interval, "v", new SingleElementPartitionChunk<>(selector));

    getDefaultQueryRunner().run(QueryPlus.wrap(query), context);

    Assert.assertTrue("Capture cache keys", cacheKeyCapture.hasCaptured());
    Assert.assertTrue("Cache key below limit", ImmutableList.copyOf(cacheKeyCapture.getValue()).size() <= limit);

    EasyMock.verify(cache);

    EasyMock.reset(cache);
    cacheKeyCapture.reset();
    EasyMock.expect(cache.getBulk(EasyMock.capture(cacheKeyCapture)))
            .andReturn(ImmutableMap.of())
            .once();
    EasyMock.replay(cache);
    client = makeClient(new ForegroundCachePopulator(JSON_MAPPER, new CachePopulatorStats(), -1), cache, 0);
    getDefaultQueryRunner().run(QueryPlus.wrap(query), context);
    EasyMock.verify(cache);
    EasyMock.verify(dataSegment);
    Assert.assertTrue("Capture cache keys", cacheKeyCapture.hasCaptured());
    Assert.assertTrue("Cache Keys empty", ImmutableList.copyOf(cacheKeyCapture.getValue()).isEmpty());
  }

  @Test
  public void testTimeseriesMergingOutOfOrderPartitions()
  {
    final Druids.TimeseriesQueryBuilder builder = Druids.newTimeseriesQueryBuilder()
                                                        .dataSource(DATA_SOURCE)
                                                        .intervals(SEG_SPEC)
                                                        .filters(DIM_FILTER)
                                                        .granularity(GRANULARITY)
                                                        .aggregators(AGGS)
                                                        .postAggregators(POST_AGGS)
                                                        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(),
        new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator())
    );

    testQueryCaching(
        runner,
        builder.build(),
        Intervals.of("2011-01-05/2011-01-10"),
        makeTimeResults(
            DateTimes.of("2011-01-05T02"), 80, 100,
            DateTimes.of("2011-01-06T02"), 420, 520,
            DateTimes.of("2011-01-07T02"), 12, 2194,
            DateTimes.of("2011-01-08T02"), 59, 201,
            DateTimes.of("2011-01-09T02"), 181, 52
        ),
        Intervals.of("2011-01-05/2011-01-10"),
        makeTimeResults(
            DateTimes.of("2011-01-05T00"), 85, 102,
            DateTimes.of("2011-01-06T00"), 412, 521,
            DateTimes.of("2011-01-07T00"), 122, 21894,
            DateTimes.of("2011-01-08T00"), 5, 20,
            DateTimes.of("2011-01-09T00"), 18, 521
        )
    );

    TimeseriesQuery query = builder
        .intervals("2011-01-05/2011-01-10")
        .aggregators(RENAMED_AGGS)
        .postAggregators(RENAMED_POST_AGGS)
        .build();
    TestHelper.assertExpectedResults(
        makeRenamedTimeResults(
            DateTimes.of("2011-01-05T00"), 85, 102,
            DateTimes.of("2011-01-05T02"), 80, 100,
            DateTimes.of("2011-01-06T00"), 412, 521,
            DateTimes.of("2011-01-06T02"), 420, 520,
            DateTimes.of("2011-01-07T00"), 122, 21894,
            DateTimes.of("2011-01-07T02"), 12, 2194,
            DateTimes.of("2011-01-08T00"), 5, 20,
            DateTimes.of("2011-01-08T02"), 59, 201,
            DateTimes.of("2011-01-09T00"), 18, 521,
            DateTimes.of("2011-01-09T02"), 181, 52
        ),
        runner.run(QueryPlus.wrap(query), new HashMap<>())
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeseriesCachingTimeZone()
  {
    final Druids.TimeseriesQueryBuilder builder = Druids.newTimeseriesQueryBuilder()
                                                        .dataSource(DATA_SOURCE)
                                                        .intervals(SEG_SPEC)
                                                        .filters(DIM_FILTER)
                                                        .granularity(PT1H_TZ_GRANULARITY)
                                                        .aggregators(AGGS)
                                                        .postAggregators(POST_AGGS)
                                                        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(), new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    testQueryCaching(
        runner,
        builder.build(),
        Intervals.of("2011-11-04/2011-11-08"),
        makeTimeResults(
            new DateTime("2011-11-04", TIMEZONE), 50, 5000,
            new DateTime("2011-11-05", TIMEZONE), 30, 6000,
            new DateTime("2011-11-06", TIMEZONE), 23, 85312,
            new DateTime("2011-11-07", TIMEZONE), 85, 102
        )
    );
    HashMap<String, List> context = new HashMap<String, List>();
    TimeseriesQuery query = builder
        .intervals("2011-11-04/2011-11-08")
        .aggregators(RENAMED_AGGS)
        .postAggregators(RENAMED_POST_AGGS)
        .build();
    TestHelper.assertExpectedResults(
        makeRenamedTimeResults(
            new DateTime("2011-11-04", TIMEZONE), 50, 5000,
            new DateTime("2011-11-05", TIMEZONE), 30, 6000,
            new DateTime("2011-11-06", TIMEZONE), 23, 85312,
            new DateTime("2011-11-07", TIMEZONE), 85, 102
        ),
        runner.run(QueryPlus.wrap(query), context)
    );
  }

  @Test
  public void testDisableUseCache()
  {
    final Druids.TimeseriesQueryBuilder builder = Druids.newTimeseriesQueryBuilder()
                                                        .dataSource(DATA_SOURCE)
                                                        .intervals(SEG_SPEC)
                                                        .filters(DIM_FILTER)
                                                        .granularity(GRANULARITY)
                                                        .aggregators(AGGS)
                                                        .postAggregators(POST_AGGS)
                                                        .context(CONTEXT);
    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(),
        new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator())
    );
    testQueryCaching(
        runner,
        1,
        true,
        builder.context(
            ImmutableMap.of(
                "useCache", "false",
                "populateCache", "true"
            )
        ).build(),
        Intervals.of("2011-01-01/2011-01-02"), makeTimeResults(DateTimes.of("2011-01-01"), 50, 5000)
    );

    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumMisses());

    cache.close(SegmentId.dummy("0_0").toString());

    testQueryCaching(
        runner,
        1,
        false,
        builder.context(
            ImmutableMap.of(
                "useCache", "false",
                "populateCache", "false"
            )
        ).build(),
        Intervals.of("2011-01-01/2011-01-02"), makeTimeResults(DateTimes.of("2011-01-01"), 50, 5000)
    );

    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumMisses());

    testQueryCaching(
        getDefaultQueryRunner(),
        1,
        false,
        builder.context(
            ImmutableMap.of(
                "useCache", "true",
                "populateCache", "false"
            )
        ).build(),
        Intervals.of("2011-01-01/2011-01-02"), makeTimeResults(DateTimes.of("2011-01-01"), 50, 5000)
    );

    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumMisses());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTopNCaching()
  {
    final TopNQueryBuilder builder = new TopNQueryBuilder()
        .dataSource(DATA_SOURCE)
        .dimension(TOP_DIM)
        .metric("imps")
        .threshold(3)
        .intervals(SEG_SPEC)
        .filters(DIM_FILTER)
        .granularity(GRANULARITY)
        .aggregators(AGGS)
        .postAggregators(POST_AGGS)
        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(),
        new TopNQueryQueryToolChest(
            new TopNQueryConfig(),
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        )
    );

    testQueryCaching(
        runner,
        builder.build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeTopNResultsWithoutRename(DateTimes.of("2011-01-01"), "a", 50, 5000, "b", 50, 4999, "c", 50, 4998),

        Intervals.of("2011-01-02/2011-01-03"),
        makeTopNResultsWithoutRename(DateTimes.of("2011-01-02"), "a", 50, 4997, "b", 50, 4996, "c", 50, 4995),

        Intervals.of("2011-01-05/2011-01-10"),
        makeTopNResultsWithoutRename(
            DateTimes.of("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09"), "c1", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeTopNResultsWithoutRename(
            DateTimes.of("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09T01"), "c2", 50, 4985, "b", 50, 4984, "c", 50, 4983
        )
    );
    HashMap<String, List> context = new HashMap<String, List>();
    TopNQuery query = builder
        .intervals("2011-01-01/2011-01-10")
        .metric("imps")
        .aggregators(RENAMED_AGGS)
        .postAggregators(DIFF_ORDER_POST_AGGS)
        .build();
    TestHelper.assertExpectedResults(
        makeRenamedTopNResults(
            DateTimes.of("2011-01-01"), "a", 50, 5000, "b", 50, 4999, "c", 50, 4998,
            DateTimes.of("2011-01-02"), "a", 50, 4997, "b", 50, 4996, "c", 50, 4995,
            DateTimes.of("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09"), "c1", 50, 4985, "b", 50, 4984, "c", 50, 4983,
            DateTimes.of("2011-01-09T01"), "c2", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),
        runner.run(QueryPlus.wrap(query), context)
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTopNCachingTimeZone()
  {
    final TopNQueryBuilder builder = new TopNQueryBuilder()
        .dataSource(DATA_SOURCE)
        .dimension(TOP_DIM)
        .metric("imps")
        .threshold(3)
        .intervals(SEG_SPEC)
        .filters(DIM_FILTER)
        .granularity(PT1H_TZ_GRANULARITY)
        .aggregators(AGGS)
        .postAggregators(POST_AGGS)
        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(),
        new TopNQueryQueryToolChest(
            new TopNQueryConfig(),
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        )
    );

    testQueryCaching(
        runner,
        builder.build(),
        Intervals.of("2011-11-04/2011-11-08"),
        makeTopNResultsWithoutRename(
            new DateTime("2011-11-04", TIMEZONE), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-11-05", TIMEZONE), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-11-06", TIMEZONE), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-11-07", TIMEZONE), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986
        )
    );
    HashMap<String, List> context = new HashMap<String, List>();
    TopNQuery query = builder
        .intervals("2011-11-04/2011-11-08")
        .metric("imps")
        .aggregators(RENAMED_AGGS)
        .postAggregators(DIFF_ORDER_POST_AGGS)
        .build();
    TestHelper.assertExpectedResults(
        makeRenamedTopNResults(

            new DateTime("2011-11-04", TIMEZONE), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-11-05", TIMEZONE), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-11-06", TIMEZONE), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-11-07", TIMEZONE), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986
        ),
        runner.run(QueryPlus.wrap(query), context)
    );
  }

  @Test
  public void testOutOfOrderSequenceMerging()
  {
    List<Sequence<Result<TopNResultValue>>> sequences =
        ImmutableList.of(
            Sequences.simple(
                makeTopNResultsWithoutRename(
                    DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
                    DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
                    DateTimes.of("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
                )
            ),
            Sequences.simple(
                makeTopNResultsWithoutRename(
                    DateTimes.of("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
                    DateTimes.of("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
                    DateTimes.of("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
                    DateTimes.of("2011-01-09T01"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
                )
            )
        );

    TestHelper.assertExpectedResults(
        makeTopNResultsWithoutRename(
            DateTimes.of("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983,
            DateTimes.of("2011-01-09T01"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),
        mergeSequences(
            new TopNQueryBuilder()
                .dataSource("test")
                .intervals("2011-01-06/2011-01-10")
                .dimension("a")
                .metric("b")
                .threshold(3)
                .aggregators(Collections.<AggregatorFactory>singletonList(new CountAggregatorFactory("b")))
                .build(),
            sequences
        )
    );
  }

  private static <T> Sequence<T> mergeSequences(Query<T> query, List<Sequence<T>> sequences)
  {
    return Sequences.simple(sequences).flatMerge(seq -> seq, query.getResultOrdering());
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testTopNCachingEmptyResults()
  {
    final TopNQueryBuilder builder = new TopNQueryBuilder()
        .dataSource(DATA_SOURCE)
        .dimension(TOP_DIM)
        .metric("imps")
        .threshold(3)
        .intervals(SEG_SPEC)
        .filters(DIM_FILTER)
        .granularity(GRANULARITY)
        .aggregators(AGGS)
        .postAggregators(POST_AGGS)
        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(), new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );
    testQueryCaching(
        runner,
        builder.build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeTopNResultsWithoutRename(),

        Intervals.of("2011-01-02/2011-01-03"),
        makeTopNResultsWithoutRename(),

        Intervals.of("2011-01-05/2011-01-10"),
        makeTopNResultsWithoutRename(
            DateTimes.of("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeTopNResultsWithoutRename(
            DateTimes.of("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09T01"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
        )
    );

    HashMap<String, List> context = new HashMap<String, List>();
    TopNQuery query = builder
        .intervals("2011-01-01/2011-01-10")
        .metric("imps")
        .aggregators(RENAMED_AGGS)
        .postAggregators(DIFF_ORDER_POST_AGGS)
        .build();
    TestHelper.assertExpectedResults(
        makeRenamedTopNResults(
            DateTimes.of("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983,
            DateTimes.of("2011-01-09T01"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),
        runner.run(QueryPlus.wrap(query), context)
    );
  }

  @Test
  public void testTopNOnPostAggMetricCaching()
  {
    final TopNQueryBuilder builder = new TopNQueryBuilder()
        .dataSource(DATA_SOURCE)
        .dimension(TOP_DIM)
        .metric("avg_imps_per_row_double")
        .threshold(3)
        .intervals(SEG_SPEC)
        .filters(DIM_FILTER)
        .granularity(GRANULARITY)
        .aggregators(AGGS)
        .postAggregators(POST_AGGS)
        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(), new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );
    testQueryCaching(
        runner,
        builder.build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeTopNResultsWithoutRename(),

        Intervals.of("2011-01-02/2011-01-03"),
        makeTopNResultsWithoutRename(),

        Intervals.of("2011-01-05/2011-01-10"),
        makeTopNResultsWithoutRename(
            DateTimes.of("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09"), "c1", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeTopNResultsWithoutRename(
            DateTimes.of("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09T01"), "c2", 50, 4985, "b", 50, 4984, "c", 50, 4983
        )
    );

    HashMap<String, List> context = new HashMap<String, List>();
    TopNQuery query = builder
        .intervals("2011-01-01/2011-01-10")
        .metric("avg_imps_per_row_double")
        .aggregators(AGGS)
        .postAggregators(DIFF_ORDER_POST_AGGS)
        .build();
    TestHelper.assertExpectedResults(
        makeTopNResultsWithoutRename(
            DateTimes.of("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            DateTimes.of("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            DateTimes.of("2011-01-09"), "c1", 50, 4985, "b", 50, 4984, "c", 50, 4983,
            DateTimes.of("2011-01-09T01"), "c2", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),
        runner.run(QueryPlus.wrap(query), context)
    );
  }

  @Test
  public void testSearchCaching()
  {
    final Druids.SearchQueryBuilder builder = Druids.newSearchQueryBuilder()
                                                    .dataSource(DATA_SOURCE)
                                                    .filters(DIM_FILTER)
                                                    .granularity(GRANULARITY)
                                                    .limit(1000)
                                                    .intervals(SEG_SPEC)
                                                    .dimensions(Collections.singletonList(TOP_DIM))
                                                    .query("how")
                                                    .context(CONTEXT);

    testQueryCaching(
        getDefaultQueryRunner(),
        builder.build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeSearchResults(TOP_DIM, DateTimes.of("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4),

        Intervals.of("2011-01-02/2011-01-03"),
        makeSearchResults(TOP_DIM, DateTimes.of("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4),

        Intervals.of("2011-01-05/2011-01-10"),
        makeSearchResults(
            TOP_DIM,
            DateTimes.of("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeSearchResults(
            TOP_DIM,
            DateTimes.of("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(), new SearchQueryQueryToolChest(
        new SearchQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        makeSearchResults(
            TOP_DIM,
            DateTimes.of("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4,
            DateTimes.of("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4,
            DateTimes.of("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4,
            DateTimes.of("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),
        runner.run(QueryPlus.wrap(builder.intervals("2011-01-01/2011-01-10").build()), context)
    );
  }

  @Test
  public void testSearchCachingRenamedOutput()
  {
    final Druids.SearchQueryBuilder builder = Druids.newSearchQueryBuilder()
                                                    .dataSource(DATA_SOURCE)
                                                    .filters(DIM_FILTER)
                                                    .granularity(GRANULARITY)
                                                    .limit(1000)
                                                    .intervals(SEG_SPEC)
                                                    .dimensions(Collections.singletonList(TOP_DIM))
                                                    .query("how")
                                                    .context(CONTEXT);

    testQueryCaching(
        getDefaultQueryRunner(),
        builder.build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeSearchResults(TOP_DIM, DateTimes.of("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4),

        Intervals.of("2011-01-02/2011-01-03"),
        makeSearchResults(TOP_DIM, DateTimes.of("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4),

        Intervals.of("2011-01-05/2011-01-10"),
        makeSearchResults(
            TOP_DIM,
            DateTimes.of("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeSearchResults(
            TOP_DIM,
            DateTimes.of("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(), new SearchQueryQueryToolChest(
        new SearchQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        makeSearchResults(
            TOP_DIM,
            DateTimes.of("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4,
            DateTimes.of("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4,
            DateTimes.of("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4,
            DateTimes.of("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),
        runner.run(QueryPlus.wrap(builder.intervals("2011-01-01/2011-01-10").build()), context)
    );
    SearchQuery query = builder
        .intervals("2011-01-01/2011-01-10")
        .dimensions(new DefaultDimensionSpec(TOP_DIM, "new_dim"))
        .build();
    TestHelper.assertExpectedResults(
        makeSearchResults(
            "new_dim",
            DateTimes.of("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4,
            DateTimes.of("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4,
            DateTimes.of("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            DateTimes.of("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            DateTimes.of("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            DateTimes.of("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            DateTimes.of("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4,
            DateTimes.of("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),
        runner.run(QueryPlus.wrap(query), context)
    );
  }

  @Test
  public void testSelectCaching()
  {
    final Set<String> dimensions = Sets.newHashSet("a");
    final Set<String> metrics = Sets.newHashSet("rows");

    Druids.SelectQueryBuilder builder = Druids.newSelectQueryBuilder()
                                              .dataSource(DATA_SOURCE)
                                              .intervals(SEG_SPEC)
                                              .filters(DIM_FILTER)
                                              .granularity(GRANULARITY)
                                              .dimensions(Collections.singletonList("a"))
                                              .metrics(Collections.singletonList("rows"))
                                              .pagingSpec(new PagingSpec(null, 3))
                                              .context(CONTEXT);

    testQueryCaching(
        getDefaultQueryRunner(),
        builder.build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeSelectResults(dimensions, metrics, DateTimes.of("2011-01-01"), ImmutableMap.of("a", "b", "rows", 1)),

        Intervals.of("2011-01-02/2011-01-03"),
        makeSelectResults(dimensions, metrics, DateTimes.of("2011-01-02"), ImmutableMap.of("a", "c", "rows", 5)),

        Intervals.of("2011-01-05/2011-01-10"),
        makeSelectResults(
            dimensions,
            metrics,
            DateTimes.of("2011-01-05"),
            DateTimes.of("2011-01-06"),
            DateTimes.of("2011-01-07"), ImmutableMap.of("a", "f", "rows", 7), ImmutableMap.of("a", "ff"),
            DateTimes.of("2011-01-08"), ImmutableMap.of("a", "g", "rows", 8),
            DateTimes.of("2011-01-09"), ImmutableMap.of("a", "h", "rows", 9)
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeSelectResults(
            dimensions,
            metrics,
            DateTimes.of("2011-01-05T01"), ImmutableMap.of("a", "d", "rows", 5),
            DateTimes.of("2011-01-06T01"), ImmutableMap.of("a", "e", "rows", 6),
            DateTimes.of("2011-01-07T01"), ImmutableMap.of("a", "f", "rows", 7),
            DateTimes.of("2011-01-08T01"), ImmutableMap.of("a", "g", "rows", 8),
            DateTimes.of("2011-01-09T01"), ImmutableMap.of("a", "h", "rows", 9)
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(),
        new SelectQueryQueryToolChest(
            JSON_MAPPER,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator(),
            SELECT_CONFIG_SUPPLIER
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        makeSelectResults(
            dimensions,
            metrics,
            DateTimes.of("2011-01-01"), ImmutableMap.of("a", "b", "rows", 1),
            DateTimes.of("2011-01-02"), ImmutableMap.of("a", "c", "rows", 5),
            DateTimes.of("2011-01-05"),
            DateTimes.of("2011-01-05T01"), ImmutableMap.of("a", "d", "rows", 5),
            DateTimes.of("2011-01-06"),
            DateTimes.of("2011-01-06T01"), ImmutableMap.of("a", "e", "rows", 6),
            DateTimes.of("2011-01-07"), ImmutableMap.of("a", "f", "rows", 7), ImmutableMap.of("a", "ff"),
            DateTimes.of("2011-01-07T01"), ImmutableMap.of("a", "f", "rows", 7),
            DateTimes.of("2011-01-08"), ImmutableMap.of("a", "g", "rows", 8),
            DateTimes.of("2011-01-08T01"), ImmutableMap.of("a", "g", "rows", 8),
            DateTimes.of("2011-01-09"), ImmutableMap.of("a", "h", "rows", 9),
            DateTimes.of("2011-01-09T01"), ImmutableMap.of("a", "h", "rows", 9)
        ),
        runner.run(QueryPlus.wrap(builder.intervals("2011-01-01/2011-01-10").build()), context)
    );
  }

  @Test
  public void testSelectCachingRenamedOutputName()
  {
    final Set<String> dimensions = Sets.newHashSet("a");
    final Set<String> metrics = Sets.newHashSet("rows");

    Druids.SelectQueryBuilder builder = Druids.newSelectQueryBuilder()
                                              .dataSource(DATA_SOURCE)
                                              .intervals(SEG_SPEC)
                                              .filters(DIM_FILTER)
                                              .granularity(GRANULARITY)
                                              .dimensions(Collections.singletonList("a"))
                                              .metrics(Collections.singletonList("rows"))
                                              .pagingSpec(new PagingSpec(null, 3))
                                              .context(CONTEXT);

    testQueryCaching(
        getDefaultQueryRunner(),
        builder.build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeSelectResults(dimensions, metrics, DateTimes.of("2011-01-01"), ImmutableMap.of("a", "b", "rows", 1)),

        Intervals.of("2011-01-02/2011-01-03"),
        makeSelectResults(dimensions, metrics, DateTimes.of("2011-01-02"), ImmutableMap.of("a", "c", "rows", 5)),

        Intervals.of("2011-01-05/2011-01-10"),
        makeSelectResults(
            dimensions,
            metrics,
            DateTimes.of("2011-01-05"), ImmutableMap.of("a", "d", "rows", 5),
            DateTimes.of("2011-01-06"), ImmutableMap.of("a", "e", "rows", 6),
            DateTimes.of("2011-01-07"), ImmutableMap.of("a", "f", "rows", 7),
            DateTimes.of("2011-01-08"), ImmutableMap.of("a", "g", "rows", 8),
            DateTimes.of("2011-01-09"), ImmutableMap.of("a", "h", "rows", 9)
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeSelectResults(
            dimensions,
            metrics,
            DateTimes.of("2011-01-05T01"), ImmutableMap.of("a", "d", "rows", 5),
            DateTimes.of("2011-01-06T01"), ImmutableMap.of("a", "e", "rows", 6),
            DateTimes.of("2011-01-07T01"), ImmutableMap.of("a", "f", "rows", 7),
            DateTimes.of("2011-01-08T01"), ImmutableMap.of("a", "g", "rows", 8),
            DateTimes.of("2011-01-09T01"), ImmutableMap.of("a", "h", "rows", 9)
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(),
        new SelectQueryQueryToolChest(
            JSON_MAPPER,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator(),
            SELECT_CONFIG_SUPPLIER
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        makeSelectResults(
            dimensions,
            metrics,
            DateTimes.of("2011-01-01"), ImmutableMap.of("a", "b", "rows", 1),
            DateTimes.of("2011-01-02"), ImmutableMap.of("a", "c", "rows", 5),
            DateTimes.of("2011-01-05"), ImmutableMap.of("a", "d", "rows", 5),
            DateTimes.of("2011-01-05T01"), ImmutableMap.of("a", "d", "rows", 5),
            DateTimes.of("2011-01-06"), ImmutableMap.of("a", "e", "rows", 6),
            DateTimes.of("2011-01-06T01"), ImmutableMap.of("a", "e", "rows", 6),
            DateTimes.of("2011-01-07"), ImmutableMap.of("a", "f", "rows", 7),
            DateTimes.of("2011-01-07T01"), ImmutableMap.of("a", "f", "rows", 7),
            DateTimes.of("2011-01-08"), ImmutableMap.of("a", "g", "rows", 8),
            DateTimes.of("2011-01-08T01"), ImmutableMap.of("a", "g", "rows", 8),
            DateTimes.of("2011-01-09"), ImmutableMap.of("a", "h", "rows", 9),
            DateTimes.of("2011-01-09T01"), ImmutableMap.of("a", "h", "rows", 9)
        ),
        runner.run(QueryPlus.wrap(builder.intervals("2011-01-01/2011-01-10").build()), context)
    );

    SelectQuery query = builder
        .intervals("2011-01-01/2011-01-10")
        .dimensionSpecs(Collections.singletonList(new DefaultDimensionSpec("a", "a2")))
        .build();
    TestHelper.assertExpectedResults(
        makeSelectResults(
            dimensions,
            metrics,
            DateTimes.of("2011-01-01"), ImmutableMap.of("a2", "b", "rows", 1),
            DateTimes.of("2011-01-02"), ImmutableMap.of("a2", "c", "rows", 5),
            DateTimes.of("2011-01-05"), ImmutableMap.of("a2", "d", "rows", 5),
            DateTimes.of("2011-01-05T01"), ImmutableMap.of("a2", "d", "rows", 5),
            DateTimes.of("2011-01-06"), ImmutableMap.of("a2", "e", "rows", 6),
            DateTimes.of("2011-01-06T01"), ImmutableMap.of("a2", "e", "rows", 6),
            DateTimes.of("2011-01-07"), ImmutableMap.of("a2", "f", "rows", 7),
            DateTimes.of("2011-01-07T01"), ImmutableMap.of("a2", "f", "rows", 7),
            DateTimes.of("2011-01-08"), ImmutableMap.of("a2", "g", "rows", 8),
            DateTimes.of("2011-01-08T01"), ImmutableMap.of("a2", "g", "rows", 8),
            DateTimes.of("2011-01-09"), ImmutableMap.of("a2", "h", "rows", 9),
            DateTimes.of("2011-01-09T01"), ImmutableMap.of("a2", "h", "rows", 9)
        ),
        runner.run(QueryPlus.wrap(query), context)
    );
  }

  @Test
  public void testGroupByCaching()
  {
    List<AggregatorFactory> aggsWithUniques = ImmutableList.<AggregatorFactory>builder()
        .addAll(AGGS)
        .add(new HyperUniquesAggregatorFactory("uniques", "uniques"))
        .build();

    final HashFunction hashFn = Hashing.murmur3_128();

    GroupByQuery.Builder builder = new GroupByQuery.Builder()
        .setDataSource(DATA_SOURCE)
        .setQuerySegmentSpec(SEG_SPEC)
        .setDimFilter(DIM_FILTER)
        .setGranularity(GRANULARITY).setDimensions(new DefaultDimensionSpec("a", "a"))
        .setAggregatorSpecs(aggsWithUniques)
        .setPostAggregatorSpecs(POST_AGGS)
        .setContext(CONTEXT);

    final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.add(hashFn.hashString("abc123", StandardCharsets.UTF_8).asBytes());
    collector.add(hashFn.hashString("123abc", StandardCharsets.UTF_8).asBytes());

    final GroupByQuery query = builder.build();

    testQueryCaching(
        getDefaultQueryRunner(),
        query,
        Intervals.of("2011-01-01/2011-01-02"),
        makeGroupByResults(
            DateTimes.of("2011-01-01"),
            ImmutableMap.of("a", "a", "rows", 1, "imps", 1, "impers", 1, "uniques", collector)
        ),

        Intervals.of("2011-01-02/2011-01-03"),
        makeGroupByResults(
            DateTimes.of("2011-01-02"),
            ImmutableMap.of("a", "b", "rows", 2, "imps", 2, "impers", 2, "uniques", collector)
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeGroupByResults(
            DateTimes.of("2011-01-05"),
            ImmutableMap.of("a", "c", "rows", 3, "imps", 3, "impers", 3, "uniques", collector),
            DateTimes.of("2011-01-06"),
            ImmutableMap.of("a", "d", "rows", 4, "imps", 4, "impers", 4, "uniques", collector),
            DateTimes.of("2011-01-07"),
            ImmutableMap.of("a", "e", "rows", 5, "imps", 5, "impers", 5, "uniques", collector),
            DateTimes.of("2011-01-08"),
            ImmutableMap.of("a", "f", "rows", 6, "imps", 6, "impers", 6, "uniques", collector),
            DateTimes.of("2011-01-09"),
            ImmutableMap.of("a", "g", "rows", 7, "imps", 7, "impers", 7, "uniques", collector)
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeGroupByResults(
            DateTimes.of("2011-01-05T01"),
            ImmutableMap.of("a", "c", "rows", 3, "imps", 3, "impers", 3, "uniques", collector),
            DateTimes.of("2011-01-06T01"),
            ImmutableMap.of("a", "d", "rows", 4, "imps", 4, "impers", 4, "uniques", collector),
            DateTimes.of("2011-01-07T01"),
            ImmutableMap.of("a", "e", "rows", 5, "imps", 5, "impers", 5, "uniques", collector),
            DateTimes.of("2011-01-08T01"),
            ImmutableMap.of("a", "f", "rows", 6, "imps", 6, "impers", 6, "uniques", collector),
            DateTimes.of("2011-01-09T01"),
            ImmutableMap.of("a", "g", "rows", 7, "imps", 7, "impers", 7, "uniques", collector)
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(),
        WAREHOUSE.getToolChest(query)
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedObjects(
        makeGroupByResults(
            DateTimes.of("2011-01-05T"),
            ImmutableMap.of("a", "c", "rows", 3, "imps", 3, "impers", 3, "uniques", collector),
            DateTimes.of("2011-01-05T01"),
            ImmutableMap.of("a", "c", "rows", 3, "imps", 3, "impers", 3, "uniques", collector),
            DateTimes.of("2011-01-06T"),
            ImmutableMap.of("a", "d", "rows", 4, "imps", 4, "impers", 4, "uniques", collector),
            DateTimes.of("2011-01-06T01"),
            ImmutableMap.of("a", "d", "rows", 4, "imps", 4, "impers", 4, "uniques", collector),
            DateTimes.of("2011-01-07T"),
            ImmutableMap.of("a", "e", "rows", 5, "imps", 5, "impers", 5, "uniques", collector),
            DateTimes.of("2011-01-07T01"),
            ImmutableMap.of("a", "e", "rows", 5, "imps", 5, "impers", 5, "uniques", collector),
            DateTimes.of("2011-01-08T"),
            ImmutableMap.of("a", "f", "rows", 6, "imps", 6, "impers", 6, "uniques", collector),
            DateTimes.of("2011-01-08T01"),
            ImmutableMap.of("a", "f", "rows", 6, "imps", 6, "impers", 6, "uniques", collector),
            DateTimes.of("2011-01-09T"),
            ImmutableMap.of("a", "g", "rows", 7, "imps", 7, "impers", 7, "uniques", collector),
            DateTimes.of("2011-01-09T01"),
            ImmutableMap.of("a", "g", "rows", 7, "imps", 7, "impers", 7, "uniques", collector)
        ),
        runner.run(QueryPlus.wrap(builder.setInterval("2011-01-05/2011-01-10").build()), context),
        ""
    );
  }

  @Test
  public void testTimeBoundaryCaching()
  {
    testQueryCaching(
        getDefaultQueryRunner(),
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-01"), DateTimes.of("2011-01-01"), DateTimes.of("2011-01-02")),

        Intervals.of("2011-01-01/2011-01-03"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-02"), DateTimes.of("2011-01-02"), DateTimes.of("2011-01-03")),

        Intervals.of("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-05"), DateTimes.of("2011-01-05"), DateTimes.of("2011-01-10")),

        Intervals.of("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-05T01"), DateTimes.of("2011-01-05T01"), DateTimes.of("2011-01-10"))
    );

    testQueryCaching(
        getDefaultQueryRunner(),
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .bound(TimeBoundaryQuery.MAX_TIME)
              .build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-01"), null, DateTimes.of("2011-01-02")),

        Intervals.of("2011-01-01/2011-01-03"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-02"), null, DateTimes.of("2011-01-03")),

        Intervals.of("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-05"), null, DateTimes.of("2011-01-10")),

        Intervals.of("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-05T01"), null, DateTimes.of("2011-01-10"))
    );

    testQueryCaching(
        getDefaultQueryRunner(),
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .bound(TimeBoundaryQuery.MIN_TIME)
              .build(),
        Intervals.of("2011-01-01/2011-01-02"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-01"), DateTimes.of("2011-01-01"), null),

        Intervals.of("2011-01-01/2011-01-03"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-02"), DateTimes.of("2011-01-02"), null),

        Intervals.of("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-05"), DateTimes.of("2011-01-05"), null),

        Intervals.of("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("2011-01-05T01"), DateTimes.of("2011-01-05T01"), null)
    );
  }

  @Test
  public void testTimeSeriesWithFilter()
  {
    DimFilter filter = new AndDimFilter(
        new OrDimFilter(
            new SelectorDimFilter("dim0", "1", null),
            new BoundDimFilter("dim0", "222", "333", false, false, false, null, StringComparators.LEXICOGRAPHIC)
        ),
        new AndDimFilter(
            new InDimFilter("dim1", Arrays.asList("0", "1", "2", "3", "4"), null),
            new BoundDimFilter("dim1", "0", "3", false, true, false, null, StringComparators.LEXICOGRAPHIC),
            new BoundDimFilter("dim1", "1", "9999", true, false, false, null, StringComparators.LEXICOGRAPHIC)
        )
    );

    final Druids.TimeseriesQueryBuilder builder = Druids.newTimeseriesQueryBuilder()
                                                        .dataSource(DATA_SOURCE)
                                                        .intervals(SEG_SPEC)
                                                        .filters(filter)
                                                        .granularity(GRANULARITY)
                                                        .aggregators(AGGS)
                                                        .postAggregators(POST_AGGS)
                                                        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(), new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    /*
    For dim0 (2011-01-01/2011-01-05), the combined range is {[1,1], [222,333]}, so segments [-inf,1], [1,2], [2,3], and
    [3,4] is needed
    For dim1 (2011-01-06/2011-01-10), the combined range for the bound filters is {(1,3)}, combined this with the in
    filter result in {[2,2]}, so segments [1,2] and [2,3] is needed
    */
    List<Iterable<Result<TimeseriesResultValue>>> expectedResult = Arrays.asList(
        makeTimeResults(DateTimes.of("2011-01-01"), 50, 5000,
                        DateTimes.of("2011-01-02"), 10, 1252,
                        DateTimes.of("2011-01-03"), 20, 6213,
                        DateTimes.of("2011-01-04"), 30, 743
        ),
        makeTimeResults(DateTimes.of("2011-01-07"), 60, 6020,
                        DateTimes.of("2011-01-08"), 70, 250
        )
    );

    testQueryCachingWithFilter(
        runner,
        3,
        builder.build(),
        expectedResult,
        Intervals.of("2011-01-01/2011-01-05"), makeTimeResults(DateTimes.of("2011-01-01"), 50, 5000),
        Intervals.of("2011-01-01/2011-01-05"), makeTimeResults(DateTimes.of("2011-01-02"), 10, 1252),
        Intervals.of("2011-01-01/2011-01-05"), makeTimeResults(DateTimes.of("2011-01-03"), 20, 6213),
        Intervals.of("2011-01-01/2011-01-05"), makeTimeResults(DateTimes.of("2011-01-04"), 30, 743),
        Intervals.of("2011-01-01/2011-01-05"), makeTimeResults(DateTimes.of("2011-01-05"), 40, 6000),
        Intervals.of("2011-01-06/2011-01-10"), makeTimeResults(DateTimes.of("2011-01-06"), 50, 425),
        Intervals.of("2011-01-06/2011-01-10"), makeTimeResults(DateTimes.of("2011-01-07"), 60, 6020),
        Intervals.of("2011-01-06/2011-01-10"), makeTimeResults(DateTimes.of("2011-01-08"), 70, 250),
        Intervals.of("2011-01-06/2011-01-10"), makeTimeResults(DateTimes.of("2011-01-09"), 23, 85312),
        Intervals.of("2011-01-06/2011-01-10"), makeTimeResults(DateTimes.of("2011-01-10"), 100, 512)
    );

  }

  @Test
  public void testSingleDimensionPruning()
  {
    DimFilter filter = new AndDimFilter(
        new OrDimFilter(
            new SelectorDimFilter("dim1", "a", null),
            new BoundDimFilter("dim1", "from", "to", false, false, false, null, StringComparators.LEXICOGRAPHIC)
        ),
        new AndDimFilter(
            new InDimFilter("dim2", Arrays.asList("a", "c", "e", "g"), null),
            new BoundDimFilter("dim2", "aaa", "hi", false, false, false, null, StringComparators.LEXICOGRAPHIC),
            new BoundDimFilter("dim2", "e", "zzz", true, true, false, null, StringComparators.LEXICOGRAPHIC)
        )
    );

    final Druids.TimeseriesQueryBuilder builder = Druids.newTimeseriesQueryBuilder()
                                                        .dataSource(DATA_SOURCE)
                                                        .filters(filter)
                                                        .granularity(GRANULARITY)
                                                        .intervals(SEG_SPEC)
                                                        .context(CONTEXT)
                                                        .intervals("2011-01-05/2011-01-10")
                                                        .aggregators(RENAMED_AGGS)
                                                        .postAggregators(RENAMED_POST_AGGS);

    TimeseriesQuery query = builder.build();
    Map<String, Object> context = new HashMap<>();

    final Interval interval1 = Intervals.of("2011-01-06/2011-01-07");
    final Interval interval2 = Intervals.of("2011-01-07/2011-01-08");
    final Interval interval3 = Intervals.of("2011-01-08/2011-01-09");

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(), new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    final DruidServer lastServer = servers[random.nextInt(servers.length)];
    ServerSelector selector1 = makeMockSingleDimensionSelector(lastServer, "dim1", null, "b", 1);
    ServerSelector selector2 = makeMockSingleDimensionSelector(lastServer, "dim1", "e", "f", 2);
    ServerSelector selector3 = makeMockSingleDimensionSelector(lastServer, "dim1", "hi", "zzz", 3);
    ServerSelector selector4 = makeMockSingleDimensionSelector(lastServer, "dim2", "a", "e", 4);
    ServerSelector selector5 = makeMockSingleDimensionSelector(lastServer, "dim2", null, null, 5);
    ServerSelector selector6 = makeMockSingleDimensionSelector(lastServer, "other", "b", null, 6);

    timeline.add(interval1, "v", new StringPartitionChunk<>(null, "a", 1, selector1));
    timeline.add(interval1, "v", new StringPartitionChunk<>("a", "b", 2, selector2));
    timeline.add(interval1, "v", new StringPartitionChunk<>("b", null, 3, selector3));
    timeline.add(interval2, "v", new StringPartitionChunk<>(null, "d", 4, selector4));
    timeline.add(interval2, "v", new StringPartitionChunk<>("d", null, 5, selector5));
    timeline.add(interval3, "v", new StringPartitionChunk<>(null, null, 6, selector6));

    final Capture<QueryPlus> capture = Capture.newInstance();
    final Capture<Map<String, Object>> contextCap = Capture.newInstance();

    QueryRunner mockRunner = EasyMock.createNiceMock(QueryRunner.class);
    EasyMock.expect(mockRunner.run(EasyMock.capture(capture), EasyMock.capture(contextCap)))
            .andReturn(Sequences.empty())
            .anyTimes();
    EasyMock.expect(serverView.getQueryRunner(lastServer))
            .andReturn(mockRunner)
            .anyTimes();
    EasyMock.replay(serverView);
    EasyMock.replay(mockRunner);

    List<SegmentDescriptor> descriptors = new ArrayList<>();
    descriptors.add(new SegmentDescriptor(interval1, "v", 1));
    descriptors.add(new SegmentDescriptor(interval1, "v", 3));
    descriptors.add(new SegmentDescriptor(interval2, "v", 5));
    descriptors.add(new SegmentDescriptor(interval3, "v", 6));
    MultipleSpecificSegmentSpec expected = new MultipleSpecificSegmentSpec(descriptors);

    runner.run(QueryPlus.wrap(query), context).toList();

    Assert.assertEquals(expected, ((TimeseriesQuery) capture.getValue().getQuery()).getQuerySegmentSpec());
  }

  private ServerSelector makeMockSingleDimensionSelector(
      DruidServer server,
      String dimension,
      String start,
      String end,
      int partitionNum
  )
  {
    DataSegment segment = EasyMock.createNiceMock(DataSegment.class);
    EasyMock.expect(segment.getId()).andReturn(SegmentId.dummy(DATA_SOURCE)).anyTimes();
    EasyMock.expect(segment.getShardSpec()).andReturn(new SingleDimensionShardSpec(dimension, start, end, partitionNum))
            .anyTimes();
    EasyMock.replay(segment);

    ServerSelector selector = new ServerSelector(
        segment,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
    );
    selector.addServerAndUpdateSegment(new QueryableDruidServer(server, null), segment);
    return selector;
  }

  private Iterable<Result<TimeBoundaryResultValue>> makeTimeBoundaryResult(
      DateTime timestamp,
      DateTime minTime,
      DateTime maxTime
  )
  {
    final Object value;
    if (minTime != null && maxTime != null) {
      value = ImmutableMap.of(
          TimeBoundaryQuery.MIN_TIME,
          minTime.toString(),
          TimeBoundaryQuery.MAX_TIME,
          maxTime.toString()
      );
    } else if (maxTime != null) {
      value = ImmutableMap.of(
          TimeBoundaryQuery.MAX_TIME,
          maxTime.toString()
      );
    } else {
      value = ImmutableMap.of(
          TimeBoundaryQuery.MIN_TIME,
          minTime.toString()
      );
    }

    return ImmutableList.of(
        new Result<>(
            timestamp,
            new TimeBoundaryResultValue(value)
        )
    );
  }

  public void parseResults(
      final List<Interval> queryIntervals,
      final List<List<Iterable<Result<Object>>>> expectedResults,
      Object... args
  )
  {
    if (args.length % 2 != 0) {
      throw new ISE("args.length must be divisible by two, was %d", args.length);
    }

    for (int i = 0; i < args.length; i += 2) {
      final Interval interval = (Interval) args[i];
      final Iterable<Result<Object>> results = (Iterable<Result<Object>>) args[i + 1];

      if (queryIntervals.size() > 0 && interval.equals(queryIntervals.get(queryIntervals.size() - 1))) {
        expectedResults.get(expectedResults.size() - 1).add(results);
      } else {
        queryIntervals.add(interval);
        expectedResults.add(Lists.<Iterable<Result<Object>>>newArrayList(results));
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void testQueryCachingWithFilter(
      final QueryRunner runner,
      final int numTimesToQuery,
      final Query query,
      final List<Iterable<Result<TimeseriesResultValue>>> filteredExpected,
      Object... args // does this assume query intervals must be ordered?
  )
  {
    final List<Interval> queryIntervals = Lists.newArrayListWithCapacity(args.length / 2);
    final List<List<Iterable<Result<Object>>>> expectedResults = Lists.newArrayListWithCapacity(queryIntervals.size());

    parseResults(queryIntervals, expectedResults, args);

    for (int i = 0; i < queryIntervals.size(); ++i) {
      List<Object> mocks = new ArrayList<>();
      mocks.add(serverView);

      final Interval actualQueryInterval = new Interval(
          queryIntervals.get(0).getStart(), queryIntervals.get(i).getEnd()
      );

      final List<Map<DruidServer, ServerExpectations>> serverExpectationList = populateTimeline(
          queryIntervals,
          expectedResults,
          i,
          mocks
      );

      final Map<DruidServer, ServerExpectations> finalExpectation = serverExpectationList.get(
          serverExpectationList.size() - 1
      );
      for (Map.Entry<DruidServer, ServerExpectations> entry : finalExpectation.entrySet()) {
        DruidServer server = entry.getKey();
        ServerExpectations expectations = entry.getValue();

        EasyMock.expect(serverView.getQueryRunner(server))
                .andReturn(expectations.getQueryRunner())
                .times(0, 1);

        final Capture<? extends QueryPlus> capture = new Capture();
        final Capture<? extends Map> context = new Capture();
        QueryRunner queryable = expectations.getQueryRunner();

        if (query instanceof TimeseriesQuery) {
          final List<SegmentId> segmentIds = new ArrayList<>();
          final List<Iterable<Result<TimeseriesResultValue>>> results = new ArrayList<>();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andAnswer(new IAnswer<Sequence>()
                  {
                    @Override
                    public Sequence answer()
                    {
                      return toFilteredQueryableTimeseriesResults(
                          (TimeseriesQuery) capture.getValue().getQuery(),
                          segmentIds,
                          queryIntervals,
                          results
                      );
                    }
                  })
                  .times(0, 1);
        } else {
          throw new ISE("Unknown query type[%s]", query.getClass());
        }
      }

      final Iterable<Result<Object>> expected = new ArrayList<>();
      for (int intervalNo = 0; intervalNo < i + 1; intervalNo++) {
        Iterables.addAll((List) expected, filteredExpected.get(intervalNo));
      }

      runWithMocks(
          new Runnable()
          {
            @Override
            public void run()
            {
              HashMap<String, Object> context = new HashMap<>();
              for (int i = 0; i < numTimesToQuery; ++i) {
                TestHelper.assertExpectedResults(
                    expected,
                    runner.run(
                        QueryPlus.wrap(
                            query.withQuerySegmentSpec(
                                new MultipleIntervalSegmentSpec(
                                    ImmutableList.of(
                                        actualQueryInterval
                                    )
                                )
                            )
                        ),
                        context
                    )
                );
                if (queryCompletedCallback != null) {
                  queryCompletedCallback.run();
                }
              }
            }
          },
          mocks.toArray()
      );
    }
  }

  private Sequence<Result<TimeseriesResultValue>> toFilteredQueryableTimeseriesResults(
      TimeseriesQuery query,
      List<SegmentId> segmentIds,
      List<Interval> queryIntervals,
      List<Iterable<Result<TimeseriesResultValue>>> results
  )
  {
    MultipleSpecificSegmentSpec spec = (MultipleSpecificSegmentSpec) query.getQuerySegmentSpec();
    List<Result<TimeseriesResultValue>> ret = new ArrayList<>();
    for (SegmentDescriptor descriptor : spec.getDescriptors()) {
      SegmentId id = SegmentId.dummy(
          StringUtils.format("%s_%s", queryIntervals.indexOf(descriptor.getInterval()), descriptor.getPartitionNumber())
      );
      int index = segmentIds.indexOf(id);
      if (index != -1) {
        Result result = new Result(
            results.get(index).iterator().next().getTimestamp(),
            new BySegmentResultValueClass(
                Lists.newArrayList(results.get(index)),
                id.toString(),
                descriptor.getInterval()
            )
        );
        ret.add(result);
      } else {
        throw new ISE("Descriptor %s not found in server", id);
      }
    }
    return Sequences.simple(ret);
  }

  public void testQueryCaching(QueryRunner runner, final Query query, Object... args)
  {
    testQueryCaching(runner, 3, true, query, args);
  }

  @SuppressWarnings("unchecked")
  public void testQueryCaching(
      final QueryRunner runner,
      final int numTimesToQuery,
      boolean expectBySegment,
      final Query query,
      Object... args // does this assume query intervals must be ordered?
  )
  {

    final List<Interval> queryIntervals = Lists.newArrayListWithCapacity(args.length / 2);
    final List<List<Iterable<Result<Object>>>> expectedResults = Lists.newArrayListWithCapacity(queryIntervals.size());

    parseResults(queryIntervals, expectedResults, args);

    for (int i = 0; i < queryIntervals.size(); ++i) {
      List<Object> mocks = new ArrayList<>();
      mocks.add(serverView);

      final Interval actualQueryInterval = new Interval(
          queryIntervals.get(0).getStart(), queryIntervals.get(i).getEnd()
      );

      final List<Map<DruidServer, ServerExpectations>> serverExpectationList = populateTimeline(
          queryIntervals,
          expectedResults,
          i,
          mocks
      );

      List<Capture> queryCaptures = new ArrayList<>();
      final Map<DruidServer, ServerExpectations> finalExpectation = serverExpectationList.get(
          serverExpectationList.size() - 1
      );
      for (Map.Entry<DruidServer, ServerExpectations> entry : finalExpectation.entrySet()) {
        DruidServer server = entry.getKey();
        ServerExpectations expectations = entry.getValue();


        EasyMock.expect(serverView.getQueryRunner(server))
                .andReturn(expectations.getQueryRunner())
                .once();

        final Capture<? extends QueryPlus> capture = new Capture();
        final Capture<? extends Map> context = new Capture();
        queryCaptures.add(capture);
        QueryRunner queryable = expectations.getQueryRunner();

        if (query instanceof TimeseriesQuery) {
          List<SegmentId> segmentIds = new ArrayList<>();
          List<Interval> intervals = new ArrayList<>();
          List<Iterable<Result<TimeseriesResultValue>>> results = new ArrayList<>();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableTimeseriesResults(expectBySegment, segmentIds, intervals, results))
                  .once();

        } else if (query instanceof TopNQuery) {
          List<SegmentId> segmentIds = new ArrayList<>();
          List<Interval> intervals = new ArrayList<>();
          List<Iterable<Result<TopNResultValue>>> results = new ArrayList<>();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableTopNResults(segmentIds, intervals, results))
                  .once();
        } else if (query instanceof SearchQuery) {
          List<SegmentId> segmentIds = new ArrayList<>();
          List<Interval> intervals = new ArrayList<>();
          List<Iterable<Result<SearchResultValue>>> results = new ArrayList<>();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableSearchResults(segmentIds, intervals, results))
                  .once();
        } else if (query instanceof SelectQuery) {
          List<SegmentId> segmentIds = new ArrayList<>();
          List<Interval> intervals = new ArrayList<>();
          List<Iterable<Result<SelectResultValue>>> results = new ArrayList<>();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableSelectResults(segmentIds, intervals, results))
                  .once();
        } else if (query instanceof GroupByQuery) {
          List<SegmentId> segmentIds = new ArrayList<>();
          List<Interval> intervals = new ArrayList<>();
          List<Iterable<Row>> results = new ArrayList<>();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableGroupByResults(segmentIds, intervals, results))
                  .once();
        } else if (query instanceof TimeBoundaryQuery) {
          List<SegmentId> segmentIds = new ArrayList<>();
          List<Interval> intervals = new ArrayList<>();
          List<Iterable<Result<TimeBoundaryResultValue>>> results = new ArrayList<>();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableTimeBoundaryResults(segmentIds, intervals, results))
                  .once();
        } else {
          throw new ISE("Unknown query type[%s]", query.getClass());
        }
      }

      final int expectedResultsRangeStart;
      final int expectedResultsRangeEnd;
      if (query instanceof TimeBoundaryQuery) {
        expectedResultsRangeStart = i;
        expectedResultsRangeEnd = i + 1;
      } else {
        expectedResultsRangeStart = 0;
        expectedResultsRangeEnd = i + 1;
      }

      runWithMocks(
          new Runnable()
          {
            @Override
            public void run()
            {
              HashMap<String, List> context = new HashMap<String, List>();
              for (int i = 0; i < numTimesToQuery; ++i) {
                TestHelper.assertExpectedResults(
                    new MergeIterable<>(
                        Comparators.naturalNullsFirst(),
                        FunctionalIterable
                            .create(new RangeIterable(expectedResultsRangeStart, expectedResultsRangeEnd))
                            .transformCat(
                                new Function<Integer, Iterable<Iterable<Result<Object>>>>()
                                {
                                  @Override
                                  public Iterable<Iterable<Result<Object>>> apply(@Nullable Integer input)
                                  {
                                    List<Iterable<Result<Object>>> retVal = new ArrayList<>();

                                    final Map<DruidServer, ServerExpectations> exps = serverExpectationList.get(input);
                                    for (ServerExpectations expectations : exps.values()) {
                                      for (ServerExpectation expectation : expectations) {
                                        retVal.add(expectation.getResults());
                                      }
                                    }

                                    return retVal;
                                  }
                                }
                            )
                    ),
                    runner.run(
                        QueryPlus.wrap(
                            query.withQuerySegmentSpec(
                                new MultipleIntervalSegmentSpec(ImmutableList.of(actualQueryInterval))
                            )
                        ),
                        context
                    )
                );
                if (queryCompletedCallback != null) {
                  queryCompletedCallback.run();
                }
              }
            }
          },
          mocks.toArray()
      );

      // make sure all the queries were sent down as 'bySegment'
      for (Capture queryCapture : queryCaptures) {
        QueryPlus capturedQueryPlus = (QueryPlus) queryCapture.getValue();
        Query capturedQuery = capturedQueryPlus.getQuery();
        if (expectBySegment) {
          Assert.assertEquals(true, capturedQuery.getContextValue("bySegment"));
        } else {
          Assert.assertTrue(
              capturedQuery.getContextValue("bySegment") == null ||
              capturedQuery.getContextValue("bySegment").equals(false)
          );
        }
      }
    }
  }

  private List<Map<DruidServer, ServerExpectations>> populateTimeline(
      List<Interval> queryIntervals,
      List<List<Iterable<Result<Object>>>> expectedResults,
      int numQueryIntervals,
      List<Object> mocks
  )
  {
    timeline = new VersionedIntervalTimeline<>(Ordering.natural());

    final List<Map<DruidServer, ServerExpectations>> serverExpectationList = new ArrayList<>();

    for (int k = 0; k < numQueryIntervals + 1; ++k) {
      final int numChunks = expectedResults.get(k).size();
      final TreeMap<DruidServer, ServerExpectations> serverExpectations = new TreeMap<>();
      serverExpectationList.add(serverExpectations);
      for (int j = 0; j < numChunks; ++j) {
        DruidServer lastServer = servers[random.nextInt(servers.length)];
        if (!serverExpectations.containsKey(lastServer)) {
          serverExpectations.put(lastServer, new ServerExpectations(lastServer, makeMock(mocks, QueryRunner.class)));
        }

        DataSegment mockSegment = makeMock(mocks, DataSegment.class);
        ServerExpectation<Object> expectation = new ServerExpectation<>(
            SegmentId.dummy(StringUtils.format("%s_%s", k, j)), // interval/chunk
            queryIntervals.get(k),
            mockSegment,
            expectedResults.get(k).get(j)
        );
        serverExpectations.get(lastServer).addExpectation(expectation);

        ServerSelector selector = new ServerSelector(
            expectation.getSegment(),
            new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
        );
        selector.addServerAndUpdateSegment(new QueryableDruidServer(lastServer, null), selector.getSegment());

        final ShardSpec shardSpec;
        if (numChunks == 1) {
          shardSpec = new SingleDimensionShardSpec("dimAll", null, null, 0);
        } else {
          String start = null;
          String end = null;
          if (j > 0) {
            start = String.valueOf(j);
          }
          if (j + 1 < numChunks) {
            end = String.valueOf(j + 1);
          }
          shardSpec = new SingleDimensionShardSpec("dim" + k, start, end, j);
        }
        EasyMock.expect(mockSegment.getShardSpec())
                .andReturn(shardSpec)
                .anyTimes();
        timeline.add(queryIntervals.get(k), String.valueOf(k), shardSpec.createChunk(selector));
      }
    }
    return serverExpectationList;
  }

  private Sequence<Result<TimeseriesResultValue>> toQueryableTimeseriesResults(
      boolean bySegment,
      Iterable<SegmentId> segmentIds,
      Iterable<Interval> intervals,
      Iterable<Iterable<Result<TimeseriesResultValue>>> results
  )
  {
    if (bySegment) {
      return Sequences.simple(
          FunctionalIterable
              .create(segmentIds)
              .trinaryTransform(
                  intervals,
                  results,
                  new TrinaryFn<SegmentId, Interval, Iterable<Result<TimeseriesResultValue>>, Result<TimeseriesResultValue>>()
                  {
                    @Override
                    @SuppressWarnings("unchecked")
                    public Result<TimeseriesResultValue> apply(
                        final SegmentId segmentId,
                        final Interval interval,
                        final Iterable<Result<TimeseriesResultValue>> results
                    )
                    {
                      return new Result(
                          results.iterator().next().getTimestamp(),
                          new BySegmentResultValueClass(
                              Lists.newArrayList(results),
                              segmentId.toString(),
                              interval
                          )
                      );
                    }
                  }
              )
      );
    } else {
      return Sequences.simple(Iterables.concat(results));
    }
  }

  private Sequence<Result<TopNResultValue>> toQueryableTopNResults(
      Iterable<SegmentId> segmentIds,
      Iterable<Interval> intervals,
      Iterable<Iterable<Result<TopNResultValue>>> results
  )
  {
    return Sequences.simple(
        FunctionalIterable
            .create(segmentIds)
            .trinaryTransform(
                intervals,
                results,
                new TrinaryFn<SegmentId, Interval, Iterable<Result<TopNResultValue>>, Result<TopNResultValue>>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result<TopNResultValue> apply(
                      final SegmentId segmentId,
                      final Interval interval,
                      final Iterable<Result<TopNResultValue>> results
                  )
                  {
                    return new Result(
                        interval.getStart(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId.toString(),
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Sequence<Result<SearchResultValue>> toQueryableSearchResults(
      Iterable<SegmentId> segmentIds,
      Iterable<Interval> intervals,
      Iterable<Iterable<Result<SearchResultValue>>> results
  )
  {
    return Sequences.simple(
        FunctionalIterable
            .create(segmentIds)
            .trinaryTransform(
                intervals,
                results,
                new TrinaryFn<SegmentId, Interval, Iterable<Result<SearchResultValue>>, Result<SearchResultValue>>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result<SearchResultValue> apply(
                      final SegmentId segmentId,
                      final Interval interval,
                      final Iterable<Result<SearchResultValue>> results
                  )
                  {
                    return new Result(
                        results.iterator().next().getTimestamp(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId.toString(),
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Sequence<Result<SelectResultValue>> toQueryableSelectResults(
      Iterable<SegmentId> segmentIds,
      Iterable<Interval> intervals,
      Iterable<Iterable<Result<SelectResultValue>>> results
  )
  {
    return Sequences.simple(
        FunctionalIterable
            .create(segmentIds)
            .trinaryTransform(
                intervals,
                results,
                new TrinaryFn<SegmentId, Interval, Iterable<Result<SelectResultValue>>, Result<SelectResultValue>>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result<SelectResultValue> apply(
                      final SegmentId segmentId,
                      final Interval interval,
                      final Iterable<Result<SelectResultValue>> results
                  )
                  {
                    return new Result(
                        results.iterator().next().getTimestamp(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId.toString(),
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Sequence<Result> toQueryableGroupByResults(
      Iterable<SegmentId> segmentIds,
      Iterable<Interval> intervals,
      Iterable<Iterable<Row>> results
  )
  {
    return Sequences.simple(
        FunctionalIterable
            .create(segmentIds)
            .trinaryTransform(
                intervals,
                results,
                new TrinaryFn<SegmentId, Interval, Iterable<Row>, Result>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result apply(final SegmentId segmentId, final Interval interval, final Iterable<Row> results)
                  {
                    return new Result(
                        results.iterator().next().getTimestamp(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId.toString(),
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Sequence<Result<TimeBoundaryResultValue>> toQueryableTimeBoundaryResults(
      Iterable<SegmentId> segmentIds,
      Iterable<Interval> intervals,
      Iterable<Iterable<Result<TimeBoundaryResultValue>>> results
  )
  {
    return Sequences.simple(
        FunctionalIterable
            .create(segmentIds)
            .trinaryTransform(
                intervals,
                results,
                new TrinaryFn<SegmentId, Interval, Iterable<Result<TimeBoundaryResultValue>>, Result<TimeBoundaryResultValue>>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result<TimeBoundaryResultValue> apply(
                      final SegmentId segmentId,
                      final Interval interval,
                      final Iterable<Result<TimeBoundaryResultValue>> results
                  )
                  {
                    return new Result(
                        results.iterator().next().getTimestamp(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId.toString(),
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Iterable<Result<TimeseriesResultValue>> makeTimeResults(Object... objects)
  {
    if (objects.length % 3 != 0) {
      throw new ISE("makeTimeResults must be passed arguments in groups of 3, got[%d]", objects.length);
    }

    List<Result<TimeseriesResultValue>> retVal = Lists.newArrayListWithCapacity(objects.length / 3);
    for (int i = 0; i < objects.length; i += 3) {
      double avg_impr = ((Number) objects[i + 2]).doubleValue() / ((Number) objects[i + 1]).doubleValue();
      retVal.add(
          new Result<>(
              (DateTime) objects[i],
              new TimeseriesResultValue(
                  ImmutableMap.<String, Object>builder()
                      .put("rows", objects[i + 1])
                      .put("imps", objects[i + 2])
                      .put("impers", objects[i + 2])
                      .put("avg_imps_per_row", avg_impr)
                      .put("avg_imps_per_row_half", avg_impr / 2)
                      .put("avg_imps_per_row_double", avg_impr * 2)
                      .build()
              )
          )
      );
    }
    return retVal;
  }

  private Iterable<Result<TimeseriesResultValue>> makeRenamedTimeResults(Object... objects)
  {
    if (objects.length % 3 != 0) {
      throw new ISE("makeTimeResults must be passed arguments in groups of 3, got[%d]", objects.length);
    }

    List<Result<TimeseriesResultValue>> retVal = Lists.newArrayListWithCapacity(objects.length / 3);
    for (int i = 0; i < objects.length; i += 3) {
      retVal.add(
          new Result<>(
              (DateTime) objects[i],
              new TimeseriesResultValue(
                  ImmutableMap.of(
                      "rows", objects[i + 1],
                      "imps", objects[i + 2],
                      "impers2", objects[i + 2]
                  )
              )
          )
      );
    }
    return retVal;
  }

  private Iterable<Result<TopNResultValue>> makeTopNResultsWithoutRename(Object... objects)
  {
    return makeTopNResults(
        Lists.newArrayList(
            TOP_DIM,
            "rows",
            "imps",
            "impers",
            "avg_imps_per_row",
            "avg_imps_per_row_double",
            "avg_imps_per_row_half"
        ),
        objects
    );
  }

  private Iterable<Result<TopNResultValue>> makeTopNResults(List<String> names, Object... objects)
  {
    Preconditions.checkArgument(names.size() == 7);
    List<Result<TopNResultValue>> retVal = new ArrayList<>();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List<Map<String, Object>> values = new ArrayList<>();
      while (index < objects.length && !(objects[index] instanceof DateTime)) {
        if (objects.length - index < 3) {
          throw new ISE(
              "expect 3 values for each entry in the top list, had %d values left.", objects.length - index
          );
        }
        final double imps = ((Number) objects[index + 2]).doubleValue();
        final double rows = ((Number) objects[index + 1]).doubleValue();
        values.add(
            ImmutableMap.<String, Object>builder()
                .put(names.get(0), objects[index])
                .put(names.get(1), rows)
                .put(names.get(2), imps)
                .put(names.get(3), imps)
                .put(names.get(4), imps / rows)
                .put(names.get(5), ((imps * 2) / rows))
                .put(names.get(6), (imps / (rows * 2)))
                .build()
        );
        index += 3;
      }

      retVal.add(new Result<>(timestamp, new TopNResultValue(values)));
    }
    return retVal;
  }

  private Iterable<Result<TopNResultValue>> makeRenamedTopNResults(Object... objects)
  {
    return makeTopNResults(
        Lists.newArrayList(
            TOP_DIM,
            "rows",
            "imps",
            "impers2",
            "avg_imps_per_row",
            "avg_imps_per_row_double",
            "avg_imps_per_row_half"
        ),
        objects
    );
  }

  private Iterable<Result<SearchResultValue>> makeSearchResults(String dim, Object... objects)
  {
    List<Result<SearchResultValue>> retVal = new ArrayList<>();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List<SearchHit> values = new ArrayList<>();
      while (index < objects.length && !(objects[index] instanceof DateTime)) {
        values.add(new SearchHit(dim, objects[index++].toString(), (Integer) objects[index++]));
      }

      retVal.add(new Result<>(timestamp, new SearchResultValue(values)));
    }
    return retVal;
  }

  private Iterable<Result<SelectResultValue>> makeSelectResults(
      Set<String> dimensions,
      Set<String> metrics,
      Object... objects
  )
  {
    List<Result<SelectResultValue>> retVal = new ArrayList<>();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List<EventHolder> values = new ArrayList<>();

      while (index < objects.length && !(objects[index] instanceof DateTime)) {
        values.add(new EventHolder(null, 0, (Map) objects[index++]));
      }

      Result<SelectResultValue> result = new Result<>(
          timestamp,
          new SelectResultValue(ImmutableMap.of(timestamp.toString(), 0), dimensions, metrics, values)
      );
      retVal.add(result);
    }
    return retVal;
  }

  private Iterable<Row> makeGroupByResults(Object... objects)
  {
    List<Row> retVal = new ArrayList<>();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];
      retVal.add(new MapBasedRow(timestamp, (Map<String, Object>) objects[index++]));
    }
    return retVal;
  }

  private <T> T makeMock(List<Object> mocks, Class<T> clazz)
  {
    T obj = EasyMock.createMock(clazz);
    mocks.add(obj);
    return obj;
  }

  private void runWithMocks(Runnable toRun, Object... mocks)
  {
    EasyMock.replay(mocks);

    toRun.run();

    EasyMock.verify(mocks);
    EasyMock.reset(mocks);
  }

  protected CachingClusteredClient makeClient(final CachePopulator cachePopulator)
  {
    return makeClient(cachePopulator, cache, 10);
  }

  protected CachingClusteredClient makeClient(
      final CachePopulator cachePopulator,
      final Cache cache,
      final int mergeLimit
  )
  {
    return new CachingClusteredClient(
        WAREHOUSE,
        new TimelineServerView()
        {
          @Override
          public void registerSegmentCallback(Executor exec, SegmentCallback callback)
          {
          }

          @Override
          public VersionedIntervalTimeline<String, ServerSelector> getTimeline(DataSource dataSource)
          {
            return timeline;
          }

          @Override
          public List<ImmutableDruidServer> getDruidServers()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T> QueryRunner<T> getQueryRunner(DruidServer server)
          {
            return serverView.getQueryRunner(server);
          }

          @Override
          public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
          {

          }
        },
        cache,
        JSON_MAPPER,
        cachePopulator,
        new CacheConfig()
        {
          @Override
          public boolean isPopulateCache()
          {
            return true;
          }

          @Override
          public boolean isUseCache()
          {
            return true;
          }

          @Override
          public boolean isQueryCacheable(Query query)
          {
            return true;
          }

          @Override
          public int getCacheBulkMergeLimit()
          {
            return mergeLimit;
          }
        },
        new DruidHttpClientConfig()
        {
          @Override
          public long getMaxQueuedBytes()
          {
            return 0L;
          }
        }
    );
  }

  private static class ServerExpectation<T>
  {
    private final SegmentId segmentId;
    private final Interval interval;
    private final DataSegment segment;
    private final Iterable<Result<T>> results;

    public ServerExpectation(
        SegmentId segmentId,
        Interval interval,
        DataSegment segment,
        Iterable<Result<T>> results
    )
    {
      this.segmentId = segmentId;
      this.interval = interval;
      this.segment = segment;
      this.results = results;
    }

    public SegmentId getSegmentId()
    {
      return segmentId;
    }

    public Interval getInterval()
    {
      return interval;
    }

    public DataSegment getSegment()
    {
      return new MyDataSegment();
    }

    public Iterable<Result<T>> getResults()
    {
      return results;
    }

    private class MyDataSegment extends DataSegment
    {
      private final DataSegment baseSegment = segment;

      private MyDataSegment()
      {
        super(
            "",
            Intervals.utc(0, 1),
            "",
            null,
            null,
            null,
            NoneShardSpec.instance(),
            null,
            -1
        );
      }

      @Override
      @JsonProperty
      public String getDataSource()
      {
        return baseSegment.getDataSource();
      }

      @Override
      @JsonProperty
      public Interval getInterval()
      {
        return baseSegment.getInterval();
      }

      @Override
      @JsonProperty
      public Map<String, Object> getLoadSpec()
      {
        return baseSegment.getLoadSpec();
      }

      @Override
      @JsonProperty
      public String getVersion()
      {
        return baseSegment.getVersion();
      }

      @Override
      @JsonSerialize
      @JsonProperty
      public List<String> getDimensions()
      {
        return baseSegment.getDimensions();
      }

      @Override
      @JsonSerialize
      @JsonProperty
      public List<String> getMetrics()
      {
        return baseSegment.getMetrics();
      }

      @Override
      @JsonProperty
      public ShardSpec getShardSpec()
      {
        try {
          return baseSegment.getShardSpec();
        }
        catch (IllegalStateException e) {
          return NoneShardSpec.instance();
        }
      }

      @Override
      @JsonProperty
      public long getSize()
      {
        return baseSegment.getSize();
      }

      @Override
      public SegmentId getId()
      {
        return segmentId;
      }

      @Override
      public SegmentDescriptor toDescriptor()
      {
        return baseSegment.toDescriptor();
      }

      @Override
      public int compareTo(DataSegment dataSegment)
      {
        return baseSegment.compareTo(dataSegment);
      }

      @Override
      public boolean equals(Object o)
      {
        if (!(o instanceof DataSegment)) {
          return false;
        }
        return baseSegment.equals(o);
      }

      @Override
      public int hashCode()
      {
        return baseSegment.hashCode();
      }

      @Override
      public String toString()
      {
        return baseSegment.toString();
      }
    }
  }

  private static class ServerExpectations implements Iterable<ServerExpectation>
  {
    private final DruidServer server;
    private final QueryRunner queryRunner;
    private final List<ServerExpectation> expectations = new ArrayList<>();

    public ServerExpectations(
        DruidServer server,
        QueryRunner queryRunner
    )
    {
      this.server = server;
      this.queryRunner = queryRunner;
    }

    public QueryRunner getQueryRunner()
    {
      return queryRunner;
    }

    public void addExpectation(
        ServerExpectation expectation
    )
    {
      expectations.add(expectation);
    }

    @Override
    public Iterator<ServerExpectation> iterator()
    {
      return expectations.iterator();
    }
  }

  @Test
  public void testTimeBoundaryCachingWhenTimeIsInteger()
  {
    testQueryCaching(
        getDefaultQueryRunner(),
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .build(),
        Intervals.of("1970-01-01/1970-01-02"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-01"), DateTimes.of("1970-01-01"), DateTimes.of("1970-01-02")),

        Intervals.of("1970-01-01/2011-01-03"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-02"), DateTimes.of("1970-01-02"), DateTimes.of("1970-01-03")),

        Intervals.of("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-05"), DateTimes.of("1970-01-05"), DateTimes.of("1970-01-10")),

        Intervals.of("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-05T01"), DateTimes.of("1970-01-05T01"), DateTimes.of("1970-01-10"))
    );

    testQueryCaching(
        getDefaultQueryRunner(),
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .bound(TimeBoundaryQuery.MAX_TIME)
              .build(),
        Intervals.of("1970-01-01/2011-01-02"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-01"), null, DateTimes.of("1970-01-02")),

        Intervals.of("1970-01-01/2011-01-03"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-02"), null, DateTimes.of("1970-01-03")),

        Intervals.of("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-05"), null, DateTimes.of("1970-01-10")),

        Intervals.of("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-05T01"), null, DateTimes.of("1970-01-10"))
    );

    testQueryCaching(
        getDefaultQueryRunner(),
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .bound(TimeBoundaryQuery.MIN_TIME)
              .build(),
        Intervals.of("1970-01-01/2011-01-02"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-01"), DateTimes.of("1970-01-01"), null),

        Intervals.of("1970-01-01/2011-01-03"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-02"), DateTimes.of("1970-01-02"), null),

        Intervals.of("1970-01-01/1970-01-10"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-05"), DateTimes.of("1970-01-05"), null),

        Intervals.of("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(DateTimes.of("1970-01-05T01"), DateTimes.of("1970-01-05T01"), null)
    );
  }

  @Test
  public void testGroupByCachingRenamedAggs()
  {
    GroupByQuery.Builder builder = new GroupByQuery.Builder()
        .setDataSource(DATA_SOURCE)
        .setQuerySegmentSpec(SEG_SPEC)
        .setDimFilter(DIM_FILTER)
        .setGranularity(GRANULARITY).setDimensions(new DefaultDimensionSpec("a", "output"))
        .setAggregatorSpecs(AGGS)
        .setContext(CONTEXT);

    final GroupByQuery query1 = builder.build();
    testQueryCaching(
        getDefaultQueryRunner(),
        query1,
        Intervals.of("2011-01-01/2011-01-02"),
        makeGroupByResults(
            DateTimes.of("2011-01-01"),
            ImmutableMap.of("output", "a", "rows", 1, "imps", 1, "impers", 1)
        ),

        Intervals.of("2011-01-02/2011-01-03"),
        makeGroupByResults(
            DateTimes.of("2011-01-02"),
            ImmutableMap.of("output", "b", "rows", 2, "imps", 2, "impers", 2)
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeGroupByResults(
            DateTimes.of("2011-01-05"), ImmutableMap.of("output", "c", "rows", 3, "imps", 3, "impers", 3),
            DateTimes.of("2011-01-06"), ImmutableMap.of("output", "d", "rows", 4, "imps", 4, "impers", 4),
            DateTimes.of("2011-01-07"), ImmutableMap.of("output", "e", "rows", 5, "imps", 5, "impers", 5),
            DateTimes.of("2011-01-08"), ImmutableMap.of("output", "f", "rows", 6, "imps", 6, "impers", 6),
            DateTimes.of("2011-01-09"), ImmutableMap.of("output", "g", "rows", 7, "imps", 7, "impers", 7)
        ),

        Intervals.of("2011-01-05/2011-01-10"),
        makeGroupByResults(
            DateTimes.of("2011-01-05T01"), ImmutableMap.of("output", "c", "rows", 3, "imps", 3, "impers", 3),
            DateTimes.of("2011-01-06T01"), ImmutableMap.of("output", "d", "rows", 4, "imps", 4, "impers", 4),
            DateTimes.of("2011-01-07T01"), ImmutableMap.of("output", "e", "rows", 5, "imps", 5, "impers", 5),
            DateTimes.of("2011-01-08T01"), ImmutableMap.of("output", "f", "rows", 6, "imps", 6, "impers", 6),
            DateTimes.of("2011-01-09T01"), ImmutableMap.of("output", "g", "rows", 7, "imps", 7, "impers", 7)
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        getDefaultQueryRunner(),
        WAREHOUSE.getToolChest(query1)
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedObjects(
        makeGroupByResults(
            DateTimes.of("2011-01-05T"), ImmutableMap.of("output", "c", "rows", 3, "imps", 3, "impers", 3),
            DateTimes.of("2011-01-05T01"), ImmutableMap.of("output", "c", "rows", 3, "imps", 3, "impers", 3),
            DateTimes.of("2011-01-06T"), ImmutableMap.of("output", "d", "rows", 4, "imps", 4, "impers", 4),
            DateTimes.of("2011-01-06T01"), ImmutableMap.of("output", "d", "rows", 4, "imps", 4, "impers", 4),
            DateTimes.of("2011-01-07T"), ImmutableMap.of("output", "e", "rows", 5, "imps", 5, "impers", 5),
            DateTimes.of("2011-01-07T01"), ImmutableMap.of("output", "e", "rows", 5, "imps", 5, "impers", 5),
            DateTimes.of("2011-01-08T"), ImmutableMap.of("output", "f", "rows", 6, "imps", 6, "impers", 6),
            DateTimes.of("2011-01-08T01"), ImmutableMap.of("output", "f", "rows", 6, "imps", 6, "impers", 6),
            DateTimes.of("2011-01-09T"), ImmutableMap.of("output", "g", "rows", 7, "imps", 7, "impers", 7),
            DateTimes.of("2011-01-09T01"), ImmutableMap.of("output", "g", "rows", 7, "imps", 7, "impers", 7)
        ),
        runner.run(QueryPlus.wrap(builder.setInterval("2011-01-05/2011-01-10").build()), context),
        ""
    );

    final GroupByQuery query2 = builder
        .setInterval("2011-01-05/2011-01-10").setDimensions(new DefaultDimensionSpec("a", "output2"))
        .setAggregatorSpecs(RENAMED_AGGS)
        .build();
    TestHelper.assertExpectedObjects(
        makeGroupByResults(
            DateTimes.of("2011-01-05T"), ImmutableMap.of("output2", "c", "rows", 3, "imps", 3, "impers2", 3),
            DateTimes.of("2011-01-05T01"), ImmutableMap.of("output2", "c", "rows", 3, "imps", 3, "impers2", 3),
            DateTimes.of("2011-01-06T"), ImmutableMap.of("output2", "d", "rows", 4, "imps", 4, "impers2", 4),
            DateTimes.of("2011-01-06T01"), ImmutableMap.of("output2", "d", "rows", 4, "imps", 4, "impers2", 4),
            DateTimes.of("2011-01-07T"), ImmutableMap.of("output2", "e", "rows", 5, "imps", 5, "impers2", 5),
            DateTimes.of("2011-01-07T01"), ImmutableMap.of("output2", "e", "rows", 5, "imps", 5, "impers2", 5),
            DateTimes.of("2011-01-08T"), ImmutableMap.of("output2", "f", "rows", 6, "imps", 6, "impers2", 6),
            DateTimes.of("2011-01-08T01"), ImmutableMap.of("output2", "f", "rows", 6, "imps", 6, "impers2", 6),
            DateTimes.of("2011-01-09T"), ImmutableMap.of("output2", "g", "rows", 7, "imps", 7, "impers2", 7),
            DateTimes.of("2011-01-09T01"), ImmutableMap.of("output2", "g", "rows", 7, "imps", 7, "impers2", 7)
        ),
        runner.run(QueryPlus.wrap(query2), context),
        "renamed aggregators test"
    );
  }

  @Test
  public void testIfNoneMatch()
  {
    Interval interval = Intervals.of("2016/2017");
    final DataSegment dataSegment = new DataSegment(
        "dataSource",
        interval,
        "ver",
        ImmutableMap.of(
            "type", "hdfs",
            "path", "/tmp"
        ),
        ImmutableList.of("product"),
        ImmutableList.of("visited_sum"),
        NoneShardSpec.instance(),
        9,
        12334
    );
    final ServerSelector selector = new ServerSelector(
        dataSegment,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
    );
    selector.addServerAndUpdateSegment(new QueryableDruidServer(servers[0], null), dataSegment);
    timeline.add(interval, "ver", new SingleElementPartitionChunk<>(selector));

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                    .dataSource(DATA_SOURCE)
                                    .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(interval)))
                                    .context(ImmutableMap.of("If-None-Match", "aVJV29CJY93rszVW/QBy0arWZo0="))
                                    .build();


    Map<String, Object> responseContext = new HashMap<>();

    getDefaultQueryRunner().run(QueryPlus.wrap(query), responseContext);
    Assert.assertEquals("Z/eS4rQz5v477iq7Aashr6JPZa0=", responseContext.get("ETag"));
  }

  @SuppressWarnings("unchecked")
  private QueryRunner getDefaultQueryRunner()
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(final QueryPlus queryPlus, final Map responseContext)
      {
        return client.getQueryRunnerForIntervals(queryPlus.getQuery(), queryPlus.getQuery().getIntervals())
                     .run(queryPlus, responseContext);
      }
    };
  }
}
