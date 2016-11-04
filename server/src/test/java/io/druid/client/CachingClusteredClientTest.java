/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.client.selector.HighestPriorityTierSelectorStrategy;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.RandomServerSelectorStrategy;
import io.druid.client.selector.ServerSelector;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.guava.MergeIterable;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.nary.TrinaryFn;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectResultValue;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import io.druid.query.timeboundary.TimeBoundaryResultValue;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.TestHelper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import io.druid.timeline.partition.StringPartitionChunk;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
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
 */
@RunWith(Parameterized.class)
public class CachingClusteredClientTest
{
  public static final ImmutableMap<String, Object> CONTEXT = ImmutableMap.<String, Object>of("finalize", false);
  public static final MultipleIntervalSegmentSpec SEG_SPEC = new MultipleIntervalSegmentSpec(ImmutableList.<Interval>of());
  public static final String DATA_SOURCE = "test";
  static final DefaultObjectMapper jsonMapper = new DefaultObjectMapper(new SmileFactory());

  static {
    jsonMapper.getFactory().setCodec(jsonMapper);
  }

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
  private static final List<PostAggregator> POST_AGGS = Arrays.<PostAggregator>asList(
      new ArithmeticPostAggregator(
          "avg_imps_per_row",
          "/",
          Arrays.<PostAggregator>asList(
              new FieldAccessPostAggregator("imps", "imps"),
              new FieldAccessPostAggregator("rows", "rows")
          )
      ),
      new ArithmeticPostAggregator(
          "avg_imps_per_row_double",
          "*",
          Arrays.<PostAggregator>asList(
              new FieldAccessPostAggregator("avg_imps_per_row", "avg_imps_per_row"),
              new ConstantPostAggregator("constant", 2)
          )
      ),
      new ArithmeticPostAggregator(
          "avg_imps_per_row_half",
          "/",
          Arrays.<PostAggregator>asList(
              new FieldAccessPostAggregator("avg_imps_per_row", "avg_imps_per_row"),
              new ConstantPostAggregator("constant", 2)
          )
      )
  );
  private static final List<AggregatorFactory> RENAMED_AGGS = Arrays.asList(
      new CountAggregatorFactory("rows2"),
      new LongSumAggregatorFactory("imps", "imps"),
      new LongSumAggregatorFactory("impers2", "imps")
  );
  private static final DimFilter DIM_FILTER = null;
  private static final List<PostAggregator> RENAMED_POST_AGGS = ImmutableList.of();
  private static final QueryGranularity GRANULARITY = QueryGranularities.DAY;
  private static final DateTimeZone TIMEZONE = DateTimeZone.forID("America/Los_Angeles");
  private static final QueryGranularity PT1H_TZ_GRANULARITY = new PeriodGranularity(new Period("PT1H"), null, TIMEZONE);
  private static final String TOP_DIM = "a_dim";
  static final QueryToolChestWarehouse WAREHOUSE = new MapQueryToolChestWarehouse(
      ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                  .put(
                      TimeseriesQuery.class,
                      new TimeseriesQueryQueryToolChest(
                          QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                      )
                  )
                  .put(
                      TopNQuery.class, new TopNQueryQueryToolChest(
                          new TopNQueryConfig(),
                          QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                      )
                  )
                  .put(
                      SearchQuery.class, new SearchQueryQueryToolChest(
                          new SearchQueryConfig(),
                          QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                      )
                  )
                  .put(
                      SelectQuery.class,
                      new SelectQueryQueryToolChest(
                          jsonMapper,
                          QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                      )
                  )
                  .put(
                      GroupByQuery.class,
                      GroupByQueryRunnerTest.makeQueryRunnerFactory(new GroupByQueryConfig()).getToolchest()
                  )
                  .put(TimeBoundaryQuery.class, new TimeBoundaryQueryQueryToolChest())
                  .build()
  );
  private final Random random;
  public CachingClusteredClient client;
  private Runnable queryCompletedCallback;

  protected VersionedIntervalTimeline<String, ServerSelector> timeline;
  protected TimelineServerView serverView;
  protected Cache cache;
  DruidServer[] servers;

  public CachingClusteredClientTest(int randomSeed)
  {
    this.random = new Random(randomSeed);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return Lists.transform(
        Lists.newArrayList(new RangeIterable(RANDOMNESS)),
        new Function<Integer, Object[]>()
        {
          @Override
          public Object[] apply(@Nullable Integer input)
          {
            return new Object[]{input};
          }
        }
    );
  }

  @Before
  public void setUp() throws Exception
  {
    timeline = new VersionedIntervalTimeline<>(Ordering.<String>natural());
    serverView = EasyMock.createNiceMock(TimelineServerView.class);
    cache = MapCache.create(100000);
    client = makeClient(MoreExecutors.sameThreadExecutor());

    servers = new DruidServer[]{
        new DruidServer("test1", "test1", 10, "historical", "bye", 0),
        new DruidServer("test2", "test2", 10, "historical", "bye", 0),
        new DruidServer("test3", "test3", 10, "historical", "bye", 0),
        new DruidServer("test4", "test4", 10, "historical", "bye", 0),
        new DruidServer("test5", "test5", 10, "historical", "bye", 0)
    };
  }

  @Test
  public void testOutOfOrderBackgroundCachePopulation() throws Exception
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
          MoreExecutors.sameThreadExecutor()
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
          taskQueue.addFirst(Pair.<SettableFuture, Object>of(future, task));
          return future;
        } else {
          List<Pair<SettableFuture, Object>> tasks = Lists.newArrayList(taskQueue.iterator());
          Collections.shuffle(tasks, new Random(0));

          for (final Pair<SettableFuture, Object> pair : tasks) {
            ListenableFuture future = pair.rhs instanceof Callable ?
                                      delegate.submit((Callable) pair.rhs) :
                                      delegate.submit((Runnable) pair.rhs);
            Futures.addCallback(
                future, new FutureCallback()
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
               delegate.submit((Runnable) task);
      }

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

    client = makeClient(randomizingExecutorService);

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
        client, new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    testQueryCaching(
        runner,
        builder.build(),
        new Interval("2011-01-05/2011-01-10"),
        makeTimeResults(
            new DateTime("2011-01-05"), 85, 102,
            new DateTime("2011-01-06"), 412, 521,
            new DateTime("2011-01-07"), 122, 21894,
            new DateTime("2011-01-08"), 5, 20,
            new DateTime("2011-01-09"), 18, 521
        ),
        new Interval("2011-01-10/2011-01-13"),
        makeTimeResults(
            new DateTime("2011-01-10"), 85, 102,
            new DateTime("2011-01-11"), 412, 521,
            new DateTime("2011-01-12"), 122, 21894
        )
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeseriesCaching() throws Exception
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
        client, new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    testQueryCaching(
        runner,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"), makeTimeResults(new DateTime("2011-01-01"), 50, 5000),
        new Interval("2011-01-02/2011-01-03"), makeTimeResults(new DateTime("2011-01-02"), 30, 6000),
        new Interval("2011-01-04/2011-01-05"), makeTimeResults(new DateTime("2011-01-04"), 23, 85312),

        new Interval("2011-01-05/2011-01-10"),
        makeTimeResults(
            new DateTime("2011-01-05"), 85, 102,
            new DateTime("2011-01-06"), 412, 521,
            new DateTime("2011-01-07"), 122, 21894,
            new DateTime("2011-01-08"), 5, 20,
            new DateTime("2011-01-09"), 18, 521
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeTimeResults(
            new DateTime("2011-01-05T01"), 80, 100,
            new DateTime("2011-01-06T01"), 420, 520,
            new DateTime("2011-01-07T01"), 12, 2194,
            new DateTime("2011-01-08T01"), 59, 201,
            new DateTime("2011-01-09T01"), 181, 52
        )
    );


    HashMap<String, List> context = new HashMap<String, List>();
    TestHelper.assertExpectedResults(
        makeRenamedTimeResults(
            new DateTime("2011-01-01"), 50, 5000,
            new DateTime("2011-01-02"), 30, 6000,
            new DateTime("2011-01-04"), 23, 85312,
            new DateTime("2011-01-05"), 85, 102,
            new DateTime("2011-01-05T01"), 80, 100,
            new DateTime("2011-01-06"), 412, 521,
            new DateTime("2011-01-06T01"), 420, 520,
            new DateTime("2011-01-07"), 122, 21894,
            new DateTime("2011-01-07T01"), 12, 2194,
            new DateTime("2011-01-08"), 5, 20,
            new DateTime("2011-01-08T01"), 59, 201,
            new DateTime("2011-01-09"), 18, 521,
            new DateTime("2011-01-09T01"), 181, 52
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                   .aggregators(RENAMED_AGGS)
                   .postAggregators(RENAMED_POST_AGGS)
                   .build(),
            context
        )
    );
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testCachingOverBulkLimitEnforcesLimit() throws Exception
  {
    final int limit = 10;
    final Interval interval = new Interval("2011-01-01/2011-01-02");
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
            .andReturn(ImmutableMap.<Cache.NamedKey, byte[]>of())
            .once();
    EasyMock.replay(cache);
    client = makeClient(MoreExecutors.sameThreadExecutor(), cache, limit);
    final DruidServer lastServer = servers[random.nextInt(servers.length)];
    final DataSegment dataSegment = EasyMock.createNiceMock(DataSegment.class);
    EasyMock.expect(dataSegment.getIdentifier()).andReturn(DATA_SOURCE).anyTimes();
    EasyMock.replay(dataSegment);
    final ServerSelector selector = new ServerSelector(
        dataSegment,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
    );
    selector.addServerAndUpdateSegment(new QueryableDruidServer(lastServer, null), dataSegment);
    timeline.add(interval, "v", new SingleElementPartitionChunk<>(selector));

    client.run(query, context);

    Assert.assertTrue("Capture cache keys", cacheKeyCapture.hasCaptured());
    Assert.assertTrue("Cache key below limit", ImmutableList.copyOf(cacheKeyCapture.getValue()).size() <= limit);

    EasyMock.verify(cache);

    EasyMock.reset(cache);
    cacheKeyCapture.reset();
    EasyMock.expect(cache.getBulk(EasyMock.capture(cacheKeyCapture)))
            .andReturn(ImmutableMap.<Cache.NamedKey, byte[]>of())
            .once();
    EasyMock.replay(cache);
    client = makeClient(MoreExecutors.sameThreadExecutor(), cache, 0);
    client.run(query, context);
    EasyMock.verify(cache);
    EasyMock.verify(dataSegment);
    Assert.assertTrue("Capture cache keys", cacheKeyCapture.hasCaptured());
    Assert.assertTrue("Cache Keys empty", ImmutableList.copyOf(cacheKeyCapture.getValue()).isEmpty());
  }

  @Test
  public void testTimeseriesMergingOutOfOrderPartitions() throws Exception
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
        client, new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    testQueryCaching(
        runner,
        builder.build(),
        new Interval("2011-01-05/2011-01-10"),
        makeTimeResults(
            new DateTime("2011-01-05T02"), 80, 100,
            new DateTime("2011-01-06T02"), 420, 520,
            new DateTime("2011-01-07T02"), 12, 2194,
            new DateTime("2011-01-08T02"), 59, 201,
            new DateTime("2011-01-09T02"), 181, 52
        ),
        new Interval("2011-01-05/2011-01-10"),
        makeTimeResults(
            new DateTime("2011-01-05T00"), 85, 102,
            new DateTime("2011-01-06T00"), 412, 521,
            new DateTime("2011-01-07T00"), 122, 21894,
            new DateTime("2011-01-08T00"), 5, 20,
            new DateTime("2011-01-09T00"), 18, 521
        )
    );

    TestHelper.assertExpectedResults(
        makeRenamedTimeResults(
            new DateTime("2011-01-05T00"), 85, 102,
            new DateTime("2011-01-05T02"), 80, 100,
            new DateTime("2011-01-06T00"), 412, 521,
            new DateTime("2011-01-06T02"), 420, 520,
            new DateTime("2011-01-07T00"), 122, 21894,
            new DateTime("2011-01-07T02"), 12, 2194,
            new DateTime("2011-01-08T00"), 5, 20,
            new DateTime("2011-01-08T02"), 59, 201,
            new DateTime("2011-01-09T00"), 18, 521,
            new DateTime("2011-01-09T02"), 181, 52
        ),
        runner.run(
            builder.intervals("2011-01-05/2011-01-10")
                   .aggregators(RENAMED_AGGS)
                   .postAggregators(RENAMED_POST_AGGS)
                   .build(),
            Maps.newHashMap()
        )
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeseriesCachingTimeZone() throws Exception
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
        client, new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    testQueryCaching(
        runner,
        builder.build(),
        new Interval("2011-11-04/2011-11-08"),
        makeTimeResults(
            new DateTime("2011-11-04", TIMEZONE), 50, 5000,
            new DateTime("2011-11-05", TIMEZONE), 30, 6000,
            new DateTime("2011-11-06", TIMEZONE), 23, 85312,
            new DateTime("2011-11-07", TIMEZONE), 85, 102
        )
    );
    HashMap<String, List> context = new HashMap<String, List>();
    TestHelper.assertExpectedResults(
        makeRenamedTimeResults(
            new DateTime("2011-11-04", TIMEZONE), 50, 5000,
            new DateTime("2011-11-05", TIMEZONE), 30, 6000,
            new DateTime("2011-11-06", TIMEZONE), 23, 85312,
            new DateTime("2011-11-07", TIMEZONE), 85, 102
        ),
        runner.run(
            builder.intervals("2011-11-04/2011-11-08")
                   .aggregators(RENAMED_AGGS)
                   .postAggregators(RENAMED_POST_AGGS)
                   .build(),
            context
        )
    );
  }

  @Test
  public void testDisableUseCache() throws Exception
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
        client, new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );
    testQueryCaching(
        runner,
        1,
        true,
        builder.context(
            ImmutableMap.<String, Object>of(
                "useCache", "false",
                "populateCache", "true"
            )
        ).build(),
        new Interval("2011-01-01/2011-01-02"), makeTimeResults(new DateTime("2011-01-01"), 50, 5000)
    );

    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumMisses());

    cache.close("0_0");

    testQueryCaching(
        runner,
        1,
        false,
        builder.context(
            ImmutableMap.<String, Object>of(
                "useCache", "false",
                "populateCache", "false"
            )
        ).build(),
        new Interval("2011-01-01/2011-01-02"), makeTimeResults(new DateTime("2011-01-01"), 50, 5000)
    );

    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumMisses());

    testQueryCaching(
        client,
        1,
        false,
        builder.context(
            ImmutableMap.<String, Object>of(
                "useCache", "true",
                "populateCache", "false"
            )
        ).build(),
        new Interval("2011-01-01/2011-01-02"), makeTimeResults(new DateTime("2011-01-01"), 50, 5000)
    );

    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumMisses());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTopNCaching() throws Exception
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
        client, new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    testQueryCaching(
        runner,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"),
        makeTopNResults(new DateTime("2011-01-01"), "a", 50, 5000, "b", 50, 4999, "c", 50, 4998),

        new Interval("2011-01-02/2011-01-03"),
        makeTopNResults(new DateTime("2011-01-02"), "a", 50, 4997, "b", 50, 4996, "c", 50, 4995),

        new Interval("2011-01-05/2011-01-10"),
        makeTopNResults(
            new DateTime("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09"), "c1", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeTopNResults(
            new DateTime("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09T01"), "c2", 50, 4985, "b", 50, 4984, "c", 50, 4983
        )
    );
    HashMap<String, List> context = new HashMap<String, List>();
    TestHelper.assertExpectedResults(
        makeRenamedTopNResults(
            new DateTime("2011-01-01"), "a", 50, 5000, "b", 50, 4999, "c", 50, 4998,
            new DateTime("2011-01-02"), "a", 50, 4997, "b", 50, 4996, "c", 50, 4995,
            new DateTime("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09"), "c1", 50, 4985, "b", 50, 4984, "c", 50, 4983,
            new DateTime("2011-01-09T01"), "c2", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                   .metric("imps")
                   .aggregators(RENAMED_AGGS)
                   .postAggregators(RENAMED_POST_AGGS)
                   .build(),
            context
        )
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTopNCachingTimeZone() throws Exception
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
        client, new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );

    testQueryCaching(
        runner,
        builder.build(),
        new Interval("2011-11-04/2011-11-08"),
        makeTopNResults(
            new DateTime("2011-11-04", TIMEZONE), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-11-05", TIMEZONE), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-11-06", TIMEZONE), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-11-07", TIMEZONE), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986
        )
    );
    HashMap<String, List> context = new HashMap<String, List>();
    TestHelper.assertExpectedResults(
        makeRenamedTopNResults(

            new DateTime("2011-11-04", TIMEZONE), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-11-05", TIMEZONE), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-11-06", TIMEZONE), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-11-07", TIMEZONE), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986
        ),
        runner.run(
            builder.intervals("2011-11-04/2011-11-08")
                   .metric("imps")
                   .aggregators(RENAMED_AGGS)
                   .postAggregators(RENAMED_POST_AGGS)
                   .build(),
            context
        )
    );
  }

  @Test
  public void testOutOfOrderSequenceMerging() throws Exception
  {
    List<Sequence<Result<TopNResultValue>>> sequences =
        ImmutableList.of(
            Sequences.simple(
                makeTopNResults(
                    new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
                    new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
                    new DateTime("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
                )
            ),
            Sequences.simple(
                makeTopNResults(
                    new DateTime("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
                    new DateTime("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
                    new DateTime("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
                    new DateTime("2011-01-09T01"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
                )
            )
        );

    TestHelper.assertExpectedResults(
        makeTopNResults(
            new DateTime("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983,
            new DateTime("2011-01-09T01"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),
        client.mergeCachedAndUncachedSequences(
            new TopNQueryBuilder()
                .dataSource("test")
                .intervals("2011-01-06/2011-01-10")
                .dimension("a")
                .metric("b")
                .threshold(3)
                .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("b")))
                .build(),
            sequences
        )
    );
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testTopNCachingEmptyResults() throws Exception
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
        client, new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );
    testQueryCaching(
        runner,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"),
        makeTopNResults(),

        new Interval("2011-01-02/2011-01-03"),
        makeTopNResults(),

        new Interval("2011-01-05/2011-01-10"),
        makeTopNResults(
            new DateTime("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeTopNResults(
            new DateTime("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09T01"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
        )
    );

    HashMap<String, List> context = new HashMap<String, List>();
    TestHelper.assertExpectedResults(
        makeRenamedTopNResults(
            new DateTime("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983,
            new DateTime("2011-01-09T01"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                   .metric("imps")
                   .aggregators(RENAMED_AGGS)
                   .postAggregators(RENAMED_POST_AGGS)
                   .build(),
            context
        )
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
        client, new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );
    testQueryCaching(
        runner,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"),
        makeTopNResults(),

        new Interval("2011-01-02/2011-01-03"),
        makeTopNResults(),

        new Interval("2011-01-05/2011-01-10"),
        makeTopNResults(
            new DateTime("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09"), "c1", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeTopNResults(
            new DateTime("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09T01"), "c2", 50, 4985, "b", 50, 4984, "c", 50, 4983
        )
    );

    HashMap<String, List> context = new HashMap<String, List>();
    TestHelper.assertExpectedResults(
        makeTopNResults(
            new DateTime("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-05T01"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
            new DateTime("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-06T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-07T01"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
            new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-08T01"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
            new DateTime("2011-01-09"), "c1", 50, 4985, "b", 50, 4984, "c", 50, 4983,
            new DateTime("2011-01-09T01"), "c2", 50, 4985, "b", 50, 4984, "c", 50, 4983
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                   .metric("avg_imps_per_row_double")
                   .aggregators(AGGS)
                   .postAggregators(POST_AGGS)
                   .build(),
            context
        )
    );
  }

  @Test
  public void testSearchCaching() throws Exception
  {
    final Druids.SearchQueryBuilder builder = Druids.newSearchQueryBuilder()
                                                    .dataSource(DATA_SOURCE)
                                                    .filters(DIM_FILTER)
                                                    .granularity(GRANULARITY)
                                                    .limit(1000)
                                                    .intervals(SEG_SPEC)
                                                    .dimensions(Arrays.asList(TOP_DIM))
                                                    .query("how")
                                                    .context(CONTEXT);

    testQueryCaching(
        client,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"),
        makeSearchResults(TOP_DIM, new DateTime("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4),

        new Interval("2011-01-02/2011-01-03"),
        makeSearchResults(TOP_DIM, new DateTime("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4),

        new Interval("2011-01-05/2011-01-10"),
        makeSearchResults(
            TOP_DIM,
            new DateTime("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeSearchResults(
            TOP_DIM,
            new DateTime("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        client, new SearchQueryQueryToolChest(
        new SearchQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        makeSearchResults(
            TOP_DIM,
            new DateTime("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4,
            new DateTime("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4,
            new DateTime("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4,
            new DateTime("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                   .build(),
            context
        )
    );
  }

  @Test
  public void testSearchCachingRenamedOutput() throws Exception
  {
    final Druids.SearchQueryBuilder builder = Druids.newSearchQueryBuilder()
        .dataSource(DATA_SOURCE)
        .filters(DIM_FILTER)
        .granularity(GRANULARITY)
        .limit(1000)
        .intervals(SEG_SPEC)
        .dimensions(Arrays.asList(TOP_DIM))
        .query("how")
        .context(CONTEXT);

    testQueryCaching(
        client,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"),
        makeSearchResults(TOP_DIM, new DateTime("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4),

        new Interval("2011-01-02/2011-01-03"),
        makeSearchResults(TOP_DIM, new DateTime("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4),

        new Interval("2011-01-05/2011-01-10"),
        makeSearchResults(
            TOP_DIM,
            new DateTime("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeSearchResults(
            TOP_DIM,
            new DateTime("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        client, new SearchQueryQueryToolChest(
        new SearchQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        makeSearchResults(
            TOP_DIM,
            new DateTime("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4,
            new DateTime("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4,
            new DateTime("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4,
            new DateTime("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                .build(),
            context
        )
    );
    TestHelper.assertExpectedResults(
        makeSearchResults(
            "new_dim",
            new DateTime("2011-01-01"), "how", 1, "howdy", 2, "howwwwww", 3, "howwy", 4,
            new DateTime("2011-01-02"), "how1", 1, "howdy1", 2, "howwwwww1", 3, "howwy1", 4,
            new DateTime("2011-01-05"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-05T01"), "how2", 1, "howdy2", 2, "howwwwww2", 3, "howww2", 4,
            new DateTime("2011-01-06"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-06T01"), "how3", 1, "howdy3", 2, "howwwwww3", 3, "howww3", 4,
            new DateTime("2011-01-07"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-07T01"), "how4", 1, "howdy4", 2, "howwwwww4", 3, "howww4", 4,
            new DateTime("2011-01-08"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-08T01"), "how5", 1, "howdy5", 2, "howwwwww5", 3, "howww5", 4,
            new DateTime("2011-01-09"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4,
            new DateTime("2011-01-09T01"), "how6", 1, "howdy6", 2, "howwwwww6", 3, "howww6", 4
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                .dimensions(new DefaultDimensionSpec(
                    TOP_DIM,
                    "new_dim"
                ))
                .build(),
            context
        )
    );
  }

  @Test
  public void testSelectCaching() throws Exception
  {
    final Set<String> dimensions = Sets.<String>newHashSet("a");
    final Set<String> metrics = Sets.<String>newHashSet("rows");

    Druids.SelectQueryBuilder builder = Druids.newSelectQueryBuilder()
                                              .dataSource(DATA_SOURCE)
                                              .intervals(SEG_SPEC)
                                              .filters(DIM_FILTER)
                                              .granularity(GRANULARITY)
                                              .dimensions(Arrays.asList("a"))
                                              .metrics(Arrays.asList("rows"))
                                              .pagingSpec(new PagingSpec(null, 3))
                                              .context(CONTEXT);

    testQueryCaching(
        client,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"),
        makeSelectResults(dimensions, metrics, new DateTime("2011-01-01"), ImmutableMap.of("a", "b", "rows", 1)),

        new Interval("2011-01-02/2011-01-03"),
        makeSelectResults(dimensions, metrics, new DateTime("2011-01-02"), ImmutableMap.of("a", "c", "rows", 5)),

        new Interval("2011-01-05/2011-01-10"),
        makeSelectResults(dimensions, metrics, new DateTime("2011-01-05"), ImmutableMap.of("a", "d", "rows", 5),
            new DateTime("2011-01-06"), ImmutableMap.of("a", "e", "rows", 6),
            new DateTime("2011-01-07"), ImmutableMap.of("a", "f", "rows", 7),
            new DateTime("2011-01-08"), ImmutableMap.of("a", "g", "rows", 8),
            new DateTime("2011-01-09"), ImmutableMap.of("a", "h", "rows", 9)
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeSelectResults(dimensions, metrics, new DateTime("2011-01-05T01"), ImmutableMap.of("a", "d", "rows", 5),
            new DateTime("2011-01-06T01"), ImmutableMap.of("a", "e", "rows", 6),
            new DateTime("2011-01-07T01"), ImmutableMap.of("a", "f", "rows", 7),
            new DateTime("2011-01-08T01"), ImmutableMap.of("a", "g", "rows", 8),
            new DateTime("2011-01-09T01"), ImmutableMap.of("a", "h", "rows", 9)
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        client,
        new SelectQueryQueryToolChest(
            jsonMapper,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        makeSelectResults(dimensions, metrics, new DateTime("2011-01-01"), ImmutableMap.of("a", "b", "rows", 1),
            new DateTime("2011-01-02"), ImmutableMap.of("a", "c", "rows", 5),
            new DateTime("2011-01-05"), ImmutableMap.of("a", "d", "rows", 5),
            new DateTime("2011-01-05T01"), ImmutableMap.of("a", "d", "rows", 5),
            new DateTime("2011-01-06"), ImmutableMap.of("a", "e", "rows", 6),
            new DateTime("2011-01-06T01"), ImmutableMap.of("a", "e", "rows", 6),
            new DateTime("2011-01-07"), ImmutableMap.of("a", "f", "rows", 7),
            new DateTime("2011-01-07T01"), ImmutableMap.of("a", "f", "rows", 7),
            new DateTime("2011-01-08"), ImmutableMap.of("a", "g", "rows", 8),
            new DateTime("2011-01-08T01"), ImmutableMap.of("a", "g", "rows", 8),
            new DateTime("2011-01-09"), ImmutableMap.of("a", "h", "rows", 9),
            new DateTime("2011-01-09T01"), ImmutableMap.of("a", "h", "rows", 9)
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                   .build(),
            context
        )
    );
  }

  @Test
  public void testSelectCachingRenamedOutputName() throws Exception
  {
    final Set<String> dimensions = Sets.<String>newHashSet("a");
    final Set<String> metrics = Sets.<String>newHashSet("rows");

    Druids.SelectQueryBuilder builder = Druids.newSelectQueryBuilder()
        .dataSource(DATA_SOURCE)
        .intervals(SEG_SPEC)
        .filters(DIM_FILTER)
        .granularity(GRANULARITY)
        .dimensions(Arrays.asList("a"))
        .metrics(Arrays.asList("rows"))
        .pagingSpec(new PagingSpec(null, 3))
        .context(CONTEXT);

    testQueryCaching(
        client,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"),
        makeSelectResults(dimensions, metrics, new DateTime("2011-01-01"), ImmutableMap.of("a", "b", "rows", 1)),

        new Interval("2011-01-02/2011-01-03"),
        makeSelectResults(dimensions, metrics, new DateTime("2011-01-02"), ImmutableMap.of("a", "c", "rows", 5)),

        new Interval("2011-01-05/2011-01-10"),
        makeSelectResults(
            dimensions, metrics,
            new DateTime("2011-01-05"), ImmutableMap.of("a", "d", "rows", 5),
            new DateTime("2011-01-06"), ImmutableMap.of("a", "e", "rows", 6),
            new DateTime("2011-01-07"), ImmutableMap.of("a", "f", "rows", 7),
            new DateTime("2011-01-08"), ImmutableMap.of("a", "g", "rows", 8),
            new DateTime("2011-01-09"), ImmutableMap.of("a", "h", "rows", 9)
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeSelectResults(
            dimensions, metrics,
            new DateTime("2011-01-05T01"), ImmutableMap.of("a", "d", "rows", 5),
            new DateTime("2011-01-06T01"), ImmutableMap.of("a", "e", "rows", 6),
            new DateTime("2011-01-07T01"), ImmutableMap.of("a", "f", "rows", 7),
            new DateTime("2011-01-08T01"), ImmutableMap.of("a", "g", "rows", 8),
            new DateTime("2011-01-09T01"), ImmutableMap.of("a", "h", "rows", 9)
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        client,
        new SelectQueryQueryToolChest(
            jsonMapper,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        makeSelectResults(
            dimensions, metrics,
            new DateTime("2011-01-01"), ImmutableMap.of("a", "b", "rows", 1),
            new DateTime("2011-01-02"), ImmutableMap.of("a", "c", "rows", 5),
            new DateTime("2011-01-05"), ImmutableMap.of("a", "d", "rows", 5),
            new DateTime("2011-01-05T01"), ImmutableMap.of("a", "d", "rows", 5),
            new DateTime("2011-01-06"), ImmutableMap.of("a", "e", "rows", 6),
            new DateTime("2011-01-06T01"), ImmutableMap.of("a", "e", "rows", 6),
            new DateTime("2011-01-07"), ImmutableMap.of("a", "f", "rows", 7),
            new DateTime("2011-01-07T01"), ImmutableMap.of("a", "f", "rows", 7),
            new DateTime("2011-01-08"), ImmutableMap.of("a", "g", "rows", 8),
            new DateTime("2011-01-08T01"), ImmutableMap.of("a", "g", "rows", 8),
            new DateTime("2011-01-09"), ImmutableMap.of("a", "h", "rows", 9),
            new DateTime("2011-01-09T01"), ImmutableMap.of("a", "h", "rows", 9)
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                .build(),
            context
        )
    );

    TestHelper.assertExpectedResults(
        makeSelectResults(
            dimensions, metrics,
            new DateTime("2011-01-01"), ImmutableMap.of("a2", "b", "rows", 1),
            new DateTime("2011-01-02"), ImmutableMap.of("a2", "c", "rows", 5),
            new DateTime("2011-01-05"), ImmutableMap.of("a2", "d", "rows", 5),
            new DateTime("2011-01-05T01"), ImmutableMap.of("a2", "d", "rows", 5),
            new DateTime("2011-01-06"), ImmutableMap.of("a2", "e", "rows", 6),
            new DateTime("2011-01-06T01"), ImmutableMap.of("a2", "e", "rows", 6),
            new DateTime("2011-01-07"), ImmutableMap.of("a2", "f", "rows", 7),
            new DateTime("2011-01-07T01"), ImmutableMap.of("a2", "f", "rows", 7),
            new DateTime("2011-01-08"), ImmutableMap.of("a2", "g", "rows", 8),
            new DateTime("2011-01-08T01"), ImmutableMap.of("a2", "g", "rows", 8),
            new DateTime("2011-01-09"), ImmutableMap.of("a2", "h", "rows", 9),
            new DateTime("2011-01-09T01"), ImmutableMap.of("a2", "h", "rows", 9)
        ),
        runner.run(
            builder.intervals("2011-01-01/2011-01-10")
                .dimensionSpecs(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("a", "a2")))
                .build(),
            context
        )
    );
  }

  @Test
  public void testGroupByCaching() throws Exception
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
        .setGranularity(GRANULARITY)
        .setDimensions(Arrays.<DimensionSpec>asList(new DefaultDimensionSpec("a", "a")))
        .setAggregatorSpecs(aggsWithUniques)
        .setPostAggregatorSpecs(POST_AGGS)
        .setContext(CONTEXT);

    final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.add(hashFn.hashString("abc123", Charsets.UTF_8).asBytes());
    collector.add(hashFn.hashString("123abc", Charsets.UTF_8).asBytes());

    testQueryCaching(
        client,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"),
        makeGroupByResults(
            new DateTime("2011-01-01"),
            ImmutableMap.of("a", "a", "rows", 1, "imps", 1, "impers", 1, "uniques", collector)
        ),

        new Interval("2011-01-02/2011-01-03"),
        makeGroupByResults(
            new DateTime("2011-01-02"),
            ImmutableMap.of("a", "b", "rows", 2, "imps", 2, "impers", 2, "uniques", collector)
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeGroupByResults(
            new DateTime("2011-01-05"),
            ImmutableMap.of("a", "c", "rows", 3, "imps", 3, "impers", 3, "uniques", collector),
            new DateTime("2011-01-06"),
            ImmutableMap.of("a", "d", "rows", 4, "imps", 4, "impers", 4, "uniques", collector),
            new DateTime("2011-01-07"),
            ImmutableMap.of("a", "e", "rows", 5, "imps", 5, "impers", 5, "uniques", collector),
            new DateTime("2011-01-08"),
            ImmutableMap.of("a", "f", "rows", 6, "imps", 6, "impers", 6, "uniques", collector),
            new DateTime("2011-01-09"),
            ImmutableMap.of("a", "g", "rows", 7, "imps", 7, "impers", 7, "uniques", collector)
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeGroupByResults(
            new DateTime("2011-01-05T01"),
            ImmutableMap.of("a", "c", "rows", 3, "imps", 3, "impers", 3, "uniques", collector),
            new DateTime("2011-01-06T01"),
            ImmutableMap.of("a", "d", "rows", 4, "imps", 4, "impers", 4, "uniques", collector),
            new DateTime("2011-01-07T01"),
            ImmutableMap.of("a", "e", "rows", 5, "imps", 5, "impers", 5, "uniques", collector),
            new DateTime("2011-01-08T01"),
            ImmutableMap.of("a", "f", "rows", 6, "imps", 6, "impers", 6, "uniques", collector),
            new DateTime("2011-01-09T01"),
            ImmutableMap.of("a", "g", "rows", 7, "imps", 7, "impers", 7, "uniques", collector)
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        client,
        GroupByQueryRunnerTest.makeQueryRunnerFactory(new GroupByQueryConfig()).getToolchest()
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedObjects(
        makeGroupByResults(
            new DateTime("2011-01-05T"),
            ImmutableMap.of("a", "c", "rows", 3, "imps", 3, "impers", 3, "uniques", collector),
            new DateTime("2011-01-05T01"),
            ImmutableMap.of("a", "c", "rows", 3, "imps", 3, "impers", 3, "uniques", collector),
            new DateTime("2011-01-06T"),
            ImmutableMap.of("a", "d", "rows", 4, "imps", 4, "impers", 4, "uniques", collector),
            new DateTime("2011-01-06T01"),
            ImmutableMap.of("a", "d", "rows", 4, "imps", 4, "impers", 4, "uniques", collector),
            new DateTime("2011-01-07T"),
            ImmutableMap.of("a", "e", "rows", 5, "imps", 5, "impers", 5, "uniques", collector),
            new DateTime("2011-01-07T01"),
            ImmutableMap.of("a", "e", "rows", 5, "imps", 5, "impers", 5, "uniques", collector),
            new DateTime("2011-01-08T"),
            ImmutableMap.of("a", "f", "rows", 6, "imps", 6, "impers", 6, "uniques", collector),
            new DateTime("2011-01-08T01"),
            ImmutableMap.of("a", "f", "rows", 6, "imps", 6, "impers", 6, "uniques", collector),
            new DateTime("2011-01-09T"),
            ImmutableMap.of("a", "g", "rows", 7, "imps", 7, "impers", 7, "uniques", collector),
            new DateTime("2011-01-09T01"),
            ImmutableMap.of("a", "g", "rows", 7, "imps", 7, "impers", 7, "uniques", collector)
        ),
        runner.run(
            builder.setInterval("2011-01-05/2011-01-10")
                   .build(),
            context
        ),
        ""
    );
  }

  @Test
  public void testTimeBoundaryCaching() throws Exception
  {
    testQueryCaching(
        client,
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .build(),
        new Interval("2011-01-01/2011-01-02"),
        makeTimeBoundaryResult(new DateTime("2011-01-01"), new DateTime("2011-01-01"), new DateTime("2011-01-02")),

        new Interval("2011-01-01/2011-01-03"),
        makeTimeBoundaryResult(new DateTime("2011-01-02"), new DateTime("2011-01-02"), new DateTime("2011-01-03")),

        new Interval("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("2011-01-05"), new DateTime("2011-01-05"), new DateTime("2011-01-10")),

        new Interval("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("2011-01-05T01"), new DateTime("2011-01-05T01"), new DateTime("2011-01-10"))
    );

    testQueryCaching(
        client,
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .bound(TimeBoundaryQuery.MAX_TIME)
              .build(),
        new Interval("2011-01-01/2011-01-02"),
        makeTimeBoundaryResult(new DateTime("2011-01-01"), null, new DateTime("2011-01-02")),

        new Interval("2011-01-01/2011-01-03"),
        makeTimeBoundaryResult(new DateTime("2011-01-02"), null, new DateTime("2011-01-03")),

        new Interval("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("2011-01-05"), null, new DateTime("2011-01-10")),

        new Interval("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("2011-01-05T01"), null, new DateTime("2011-01-10"))
    );

    testQueryCaching(
        client,
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .bound(TimeBoundaryQuery.MIN_TIME)
              .build(),
        new Interval("2011-01-01/2011-01-02"),
        makeTimeBoundaryResult(new DateTime("2011-01-01"), new DateTime("2011-01-01"), null),

        new Interval("2011-01-01/2011-01-03"),
        makeTimeBoundaryResult(new DateTime("2011-01-02"), new DateTime("2011-01-02"), null),

        new Interval("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("2011-01-05"), new DateTime("2011-01-05"), null),

        new Interval("2011-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("2011-01-05T01"), new DateTime("2011-01-05T01"), null)
    );
  }

  @Test
  public void testTimeSeriesWithFilter() throws Exception
  {
    DimFilter filter = Druids.newAndDimFilterBuilder()
                             .fields(
                                 Arrays.asList(
                                     Druids.newOrDimFilterBuilder().fields(
                                         Arrays.asList(
                                             new SelectorDimFilter("dim0", "1", null),
                                             new BoundDimFilter("dim0", "222", "333", false, false, false, null,
                                                                StringComparators.LEXICOGRAPHIC
                                             )
                                         )
                                     ).build(),
                                     Druids.newAndDimFilterBuilder().fields(
                                         Arrays.asList(
                                             new InDimFilter("dim1", Arrays.asList("0", "1", "2", "3", "4"), null),
                                             new BoundDimFilter("dim1", "0", "3", false, true, false, null,
                                                                StringComparators.LEXICOGRAPHIC
                                             ),
                                             new BoundDimFilter("dim1", "1", "9999", true, false, false, null,
                                                                StringComparators.LEXICOGRAPHIC
                                             )
                                         )
                                     ).build()
                                 )
                             )
                             .build();

    final Druids.TimeseriesQueryBuilder builder = Druids.newTimeseriesQueryBuilder()
                                                        .dataSource(DATA_SOURCE)
                                                        .intervals(SEG_SPEC)
                                                        .filters(filter)
                                                        .granularity(GRANULARITY)
                                                        .aggregators(AGGS)
                                                        .postAggregators(POST_AGGS)
                                                        .context(CONTEXT);

    QueryRunner runner = new FinalizeResultsQueryRunner(
        client, new TimeseriesQueryQueryToolChest(
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
        makeTimeResults(new DateTime("2011-01-01"), 50, 5000,
                        new DateTime("2011-01-02"), 10, 1252,
                        new DateTime("2011-01-03"), 20, 6213,
                        new DateTime("2011-01-04"), 30, 743),
        makeTimeResults(new DateTime("2011-01-07"), 60, 6020,
                        new DateTime("2011-01-08"), 70, 250)
    );

    testQueryCachingWithFilter(
        runner,
        3,
        builder.build(),
        expectedResult,
        new Interval("2011-01-01/2011-01-05"), makeTimeResults(new DateTime("2011-01-01"), 50, 5000),
        new Interval("2011-01-01/2011-01-05"), makeTimeResults(new DateTime("2011-01-02"), 10, 1252),
        new Interval("2011-01-01/2011-01-05"), makeTimeResults(new DateTime("2011-01-03"), 20, 6213),
        new Interval("2011-01-01/2011-01-05"), makeTimeResults(new DateTime("2011-01-04"), 30, 743),
        new Interval("2011-01-01/2011-01-05"), makeTimeResults(new DateTime("2011-01-05"), 40, 6000),
        new Interval("2011-01-06/2011-01-10"), makeTimeResults(new DateTime("2011-01-06"), 50, 425),
        new Interval("2011-01-06/2011-01-10"), makeTimeResults(new DateTime("2011-01-07"), 60, 6020),
        new Interval("2011-01-06/2011-01-10"), makeTimeResults(new DateTime("2011-01-08"), 70, 250),
        new Interval("2011-01-06/2011-01-10"), makeTimeResults(new DateTime("2011-01-09"), 23, 85312),
        new Interval("2011-01-06/2011-01-10"), makeTimeResults(new DateTime("2011-01-10"), 100, 512)
    );

  }

  @Test
  public void testSingleDimensionPruning() throws Exception
  {
    DimFilter filter = Druids.newAndDimFilterBuilder()
                             .fields(
                                 Arrays.asList(
                                     Druids.newOrDimFilterBuilder().fields(
                                         Arrays.asList(
                                             new SelectorDimFilter("dim1", "a", null),
                                             new BoundDimFilter("dim1", "from", "to", false, false, false, null,
                                                                StringComparators.LEXICOGRAPHIC
                                             )
                                         )
                                     ).build(),
                                     Druids.newAndDimFilterBuilder().fields(
                                         Arrays.asList(
                                             new InDimFilter("dim2", Arrays.asList("a", "c", "e", "g"), null),
                                             new BoundDimFilter("dim2", "aaa", "hi", false, false, false, null,
                                                                StringComparators.LEXICOGRAPHIC
                                             ),
                                             new BoundDimFilter("dim2", "e", "zzz", true, true, false, null,
                                                                StringComparators.LEXICOGRAPHIC
                                             )
                                         )
                                     ).build()
                                 )
                             )
                             .build();

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
    Map<String, List> context = new HashMap<>();

    final Interval interval1 = new Interval("2011-01-06/2011-01-07");
    final Interval interval2 = new Interval("2011-01-07/2011-01-08");
    final Interval interval3 = new Interval("2011-01-08/2011-01-09");

    QueryRunner runner = new FinalizeResultsQueryRunner(
        client, new TimeseriesQueryQueryToolChest(
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

    final Capture<TimeseriesQuery> capture = Capture.newInstance();
    final Capture<Map<String, List>> contextCap = Capture.newInstance();

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

    Sequences.toList(runner.run(
        query,
        context
    ), Lists.newArrayList());

    Assert.assertEquals(expected, capture.getValue().getQuerySegmentSpec());
  }

  private ServerSelector makeMockSingleDimensionSelector(
      DruidServer server, String dimension, String start, String end, int partitionNum)
  {
    DataSegment segment = EasyMock.createNiceMock(DataSegment.class);
    EasyMock.expect(segment.getIdentifier()).andReturn(DATA_SOURCE).anyTimes();
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
      List<Object> mocks = Lists.newArrayList();
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

        final Capture<? extends Query> capture = new Capture();
        final Capture<? extends Map> context = new Capture();
        QueryRunner queryable = expectations.getQueryRunner();

        if (query instanceof TimeseriesQuery) {
          final List<String> segmentIds = Lists.newArrayList();
          final List<Iterable<Result<TimeseriesResultValue>>> results = Lists.newArrayList();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andAnswer(new IAnswer<Sequence>()
                  {
                    @Override
                    public Sequence answer() throws Throwable
                    {
                      return toFilteredQueryableTimeseriesResults((TimeseriesQuery)capture.getValue(), segmentIds, queryIntervals, results);
                    }
                  })
                  .times(0, 1);
        } else {
          throw new ISE("Unknown query type[%s]", query.getClass());
        }
      }

      final Iterable<Result<Object>> expected = new ArrayList<>();
      for (int intervalNo = 0; intervalNo < i + 1; intervalNo++) {
        Iterables.addAll((List)expected, filteredExpected.get(intervalNo));
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
                    expected,
                    runner.run(
                        query.withQuerySegmentSpec(
                            new MultipleIntervalSegmentSpec(
                                ImmutableList.of(
                                    actualQueryInterval
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
      List<String> segmentIds,
      List<Interval> queryIntervals,
      List<Iterable<Result<TimeseriesResultValue>>> results
  )
  {
    MultipleSpecificSegmentSpec spec = (MultipleSpecificSegmentSpec)query.getQuerySegmentSpec();
    List<Result<TimeseriesResultValue>> ret = Lists.newArrayList();
    for (SegmentDescriptor descriptor : spec.getDescriptors()) {
      String id = String.format("%s_%s", queryIntervals.indexOf(descriptor.getInterval()), descriptor.getPartitionNumber());
      int index = segmentIds.indexOf(id);
      if (index != -1) {
        ret.add(new Result(
            results.get(index).iterator().next().getTimestamp(),
            new BySegmentResultValueClass(
                Lists.newArrayList(results.get(index)),
                id,
                descriptor.getInterval()
            )
        ));
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
      List<Object> mocks = Lists.newArrayList();
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

      List<Capture> queryCaptures = Lists.newArrayList();
      final Map<DruidServer, ServerExpectations> finalExpectation = serverExpectationList.get(
          serverExpectationList.size() - 1
      );
      for (Map.Entry<DruidServer, ServerExpectations> entry : finalExpectation.entrySet()) {
        DruidServer server = entry.getKey();
        ServerExpectations expectations = entry.getValue();


        EasyMock.expect(serverView.getQueryRunner(server))
                .andReturn(expectations.getQueryRunner())
                .once();

        final Capture<? extends Query> capture = new Capture();
        final Capture<? extends Map> context = new Capture();
        queryCaptures.add(capture);
        QueryRunner queryable = expectations.getQueryRunner();

        if (query instanceof TimeseriesQuery) {
          List<String> segmentIds = Lists.newArrayList();
          List<Interval> intervals = Lists.newArrayList();
          List<Iterable<Result<TimeseriesResultValue>>> results = Lists.newArrayList();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableTimeseriesResults(expectBySegment, segmentIds, intervals, results))
                  .once();

        } else if (query instanceof TopNQuery) {
          List<String> segmentIds = Lists.newArrayList();
          List<Interval> intervals = Lists.newArrayList();
          List<Iterable<Result<TopNResultValue>>> results = Lists.newArrayList();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableTopNResults(segmentIds, intervals, results))
                  .once();
        } else if (query instanceof SearchQuery) {
          List<String> segmentIds = Lists.newArrayList();
          List<Interval> intervals = Lists.newArrayList();
          List<Iterable<Result<SearchResultValue>>> results = Lists.newArrayList();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableSearchResults(segmentIds, intervals, results))
                  .once();
        } else if (query instanceof SelectQuery) {
          List<String> segmentIds = Lists.newArrayList();
          List<Interval> intervals = Lists.newArrayList();
          List<Iterable<Result<SelectResultValue>>> results = Lists.newArrayList();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableSelectResults(segmentIds, intervals, results))
                  .once();
        } else if (query instanceof GroupByQuery) {
          List<String> segmentIds = Lists.newArrayList();
          List<Interval> intervals = Lists.newArrayList();
          List<Iterable<Row>> results = Lists.newArrayList();
          for (ServerExpectation expectation : expectations) {
            segmentIds.add(expectation.getSegmentId());
            intervals.add(expectation.getInterval());
            results.add(expectation.getResults());
          }
          EasyMock.expect(queryable.run(EasyMock.capture(capture), EasyMock.capture(context)))
                  .andReturn(toQueryableGroupByResults(segmentIds, intervals, results))
                  .once();
        } else if (query instanceof TimeBoundaryQuery) {
          List<String> segmentIds = Lists.newArrayList();
          List<Interval> intervals = Lists.newArrayList();
          List<Iterable<Result<TimeBoundaryResultValue>>> results = Lists.newArrayList();
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
                        Ordering.<Result<Object>>natural().nullsFirst(),
                        FunctionalIterable
                            .create(new RangeIterable(expectedResultsRangeStart, expectedResultsRangeEnd))
                            .transformCat(
                                new Function<Integer, Iterable<Iterable<Result<Object>>>>()
                                {
                                  @Override
                                  public Iterable<Iterable<Result<Object>>> apply(@Nullable Integer input)
                                  {
                                    List<Iterable<Result<Object>>> retVal = Lists.newArrayList();

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
                        query.withQuerySegmentSpec(
                            new MultipleIntervalSegmentSpec(
                                ImmutableList.of(
                                    actualQueryInterval
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

      // make sure all the queries were sent down as 'bySegment'
      for (Capture queryCapture : queryCaptures) {
        Query capturedQuery = (Query) queryCapture.getValue();
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

    final List<Map<DruidServer, ServerExpectations>> serverExpectationList = Lists.newArrayList();

    for (int k = 0; k < numQueryIntervals + 1; ++k) {
      final int numChunks = expectedResults.get(k).size();
      final TreeMap<DruidServer, ServerExpectations> serverExpectations = Maps.newTreeMap();
      serverExpectationList.add(serverExpectations);
      for (int j = 0; j < numChunks; ++j) {
        DruidServer lastServer = servers[random.nextInt(servers.length)];
        if (!serverExpectations.containsKey(lastServer)) {
          serverExpectations.put(lastServer, new ServerExpectations(lastServer, makeMock(mocks, QueryRunner.class)));
        }

        DataSegment mockSegment = makeMock(mocks, DataSegment.class);
        ServerExpectation expectation = new ServerExpectation(
            String.format("%s_%s", k, j), // interval/chunk
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
            end = String.valueOf(j+1);
          }
          shardSpec = new SingleDimensionShardSpec("dim"+k, start, end, j);
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
      Iterable<String> segmentIds,
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
                  new TrinaryFn<String, Interval, Iterable<Result<TimeseriesResultValue>>, Result<TimeseriesResultValue>>()
                  {
                    @Override
                    @SuppressWarnings("unchecked")
                    public Result<TimeseriesResultValue> apply(
                        final String segmentId,
                        final Interval interval,
                        final Iterable<Result<TimeseriesResultValue>> results
                    )
                    {
                      return new Result(
                          results.iterator().next().getTimestamp(),
                          new BySegmentResultValueClass(
                              Lists.newArrayList(results),
                              segmentId,
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
      Iterable<String> segmentIds, Iterable<Interval> intervals, Iterable<Iterable<Result<TopNResultValue>>> results
  )
  {
    return Sequences.simple(
        FunctionalIterable
            .create(segmentIds)
            .trinaryTransform(
                intervals,
                results,
                new TrinaryFn<String, Interval, Iterable<Result<TopNResultValue>>, Result<TopNResultValue>>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result<TopNResultValue> apply(
                      final String segmentId,
                      final Interval interval,
                      final Iterable<Result<TopNResultValue>> results
                  )
                  {
                    return new Result(
                        interval.getStart(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId,
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Sequence<Result<SearchResultValue>> toQueryableSearchResults(
      Iterable<String> segmentIds, Iterable<Interval> intervals, Iterable<Iterable<Result<SearchResultValue>>> results
  )
  {
    return Sequences.simple(
        FunctionalIterable
            .create(segmentIds)
            .trinaryTransform(
                intervals,
                results,
                new TrinaryFn<String, Interval, Iterable<Result<SearchResultValue>>, Result<SearchResultValue>>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result<SearchResultValue> apply(
                      final String segmentId,
                      final Interval interval,
                      final Iterable<Result<SearchResultValue>> results
                  )
                  {
                    return new Result(
                        results.iterator().next().getTimestamp(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId,
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Sequence<Result<SelectResultValue>> toQueryableSelectResults(
      Iterable<String> segmentIds, Iterable<Interval> intervals, Iterable<Iterable<Result<SelectResultValue>>> results
  )
  {
    return Sequences.simple(
        FunctionalIterable
            .create(segmentIds)
            .trinaryTransform(
                intervals,
                results,
                new TrinaryFn<String, Interval, Iterable<Result<SelectResultValue>>, Result<SelectResultValue>>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result<SelectResultValue> apply(
                      final String segmentId,
                      final Interval interval,
                      final Iterable<Result<SelectResultValue>> results
                  )
                  {
                    return new Result(
                        results.iterator().next().getTimestamp(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId,
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Sequence<Result> toQueryableGroupByResults(
      Iterable<String> segmentIds, Iterable<Interval> intervals, Iterable<Iterable<Row>> results
  )
  {
    return Sequences.simple(
        FunctionalIterable
            .create(segmentIds)
            .trinaryTransform(
                intervals,
                results,
                new TrinaryFn<String, Interval, Iterable<Row>, Result>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result apply(
                      final String segmentId,
                      final Interval interval,
                      final Iterable<Row> results
                  )
                  {
                    return new Result(
                        results.iterator().next().getTimestamp(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId,
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Sequence<Result<TimeBoundaryResultValue>> toQueryableTimeBoundaryResults(
      Iterable<String> segmentIds,
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
                new TrinaryFn<String, Interval, Iterable<Result<TimeBoundaryResultValue>>, Result<TimeBoundaryResultValue>>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Result<TimeBoundaryResultValue> apply(
                      final String segmentId,
                      final Interval interval,
                      final Iterable<Result<TimeBoundaryResultValue>> results
                  )
                  {
                    return new Result(
                        results.iterator().next().getTimestamp(),
                        new BySegmentResultValueClass(
                            Lists.newArrayList(results),
                            segmentId,
                            interval
                        )
                    );
                  }
                }
            )
    );
  }

  private Iterable<Result<TimeseriesResultValue>> makeTimeResults
      (Object... objects)
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

  private Iterable<BySegmentResultValueClass<TimeseriesResultValue>> makeBySegmentTimeResults
      (Object... objects)
  {
    if (objects.length % 5 != 0) {
      throw new ISE("makeTimeResults must be passed arguments in groups of 5, got[%d]", objects.length);
    }

    List<BySegmentResultValueClass<TimeseriesResultValue>> retVal = Lists.newArrayListWithCapacity(objects.length / 5);
    for (int i = 0; i < objects.length; i += 5) {
      retVal.add(
          new BySegmentResultValueClass<TimeseriesResultValue>(
              Lists.newArrayList(
                  new TimeseriesResultValue(
                      ImmutableMap.of(
                          "rows", objects[i + 1],
                          "imps", objects[i + 2],
                          "impers", objects[i + 2],
                          "avg_imps_per_row",
                          ((Number) objects[i + 2]).doubleValue() / ((Number) objects[i + 1]).doubleValue()
                      )
                  )
              ),
              (String) objects[i + 3],
              (Interval) objects[i + 4]

          )
      );
    }
    return retVal;
  }

  private Iterable<Result<TimeseriesResultValue>> makeRenamedTimeResults
      (Object... objects)
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
                      "rows2", objects[i + 1],
                      "imps", objects[i + 2],
                      "impers2", objects[i + 2]
                  )
              )
          )
      );
    }
    return retVal;
  }

  private Iterable<Result<TopNResultValue>> makeTopNResults
      (Object... objects)
  {
    List<Result<TopNResultValue>> retVal = Lists.newArrayList();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List<Map<String, Object>> values = Lists.newArrayList();
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
                        .put(TOP_DIM, objects[index])
                        .put("rows", rows)
                        .put("imps", imps)
                        .put("impers", imps)
                        .put("avg_imps_per_row", imps / rows)
                        .put("avg_imps_per_row_double", ((imps * 2) / rows))
                        .put("avg_imps_per_row_half", (imps / (rows * 2)))
                        .build()
        );
        index += 3;
      }

      retVal.add(new Result<>(timestamp, new TopNResultValue(values)));
    }
    return retVal;
  }

  private Iterable<Result<TopNResultValue>> makeRenamedTopNResults
      (Object... objects)
  {
    List<Result<TopNResultValue>> retVal = Lists.newArrayList();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List<Map<String, Object>> values = Lists.newArrayList();
      while (index < objects.length && !(objects[index] instanceof DateTime)) {
        if (objects.length - index < 3) {
          throw new ISE(
              "expect 3 values for each entry in the top list, had %d values left.", objects.length - index
          );
        }
        final double imps = ((Number) objects[index + 2]).doubleValue();
        final double rows = ((Number) objects[index + 1]).doubleValue();
        values.add(
            ImmutableMap.of(
                TOP_DIM, objects[index],
                "rows2", rows,
                "imps", imps,
                "impers2", imps
            )
        );
        index += 3;
      }

      retVal.add(new Result<>(timestamp, new TopNResultValue(values)));
    }
    return retVal;
  }

  private Iterable<Result<SearchResultValue>> makeSearchResults
      (String dim, Object... objects)
  {
    List<Result<SearchResultValue>> retVal = Lists.newArrayList();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List<SearchHit> values = Lists.newArrayList();
      while (index < objects.length && !(objects[index] instanceof DateTime)) {
        values.add(new SearchHit(dim, objects[index++].toString(), (Integer) objects[index++]));
      }

      retVal.add(new Result<>(timestamp, new SearchResultValue(values)));
    }
    return retVal;
  }

  private Iterable<Result<SelectResultValue>> makeSelectResults(Set<String> dimensions, Set<String> metrics, Object... objects)
  {
    List<Result<SelectResultValue>> retVal = Lists.newArrayList();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List<EventHolder> values = Lists.newArrayList();

      while (index < objects.length && !(objects[index] instanceof DateTime)) {
        values.add(new EventHolder(null, 0, (Map) objects[index++]));
      }

      retVal.add(new Result<>(
          timestamp,
          new SelectResultValue(null, dimensions, metrics, values)
      ));
    }
    return retVal;
  }

  private Iterable<Row> makeGroupByResults(Object... objects)
  {
    List<Row> retVal = Lists.newArrayList();
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

  protected CachingClusteredClient makeClient(final ListeningExecutorService backgroundExecutorService)
  {
    return makeClient(backgroundExecutorService, cache, 10);
  }

  protected CachingClusteredClient makeClient(
      final ListeningExecutorService backgroundExecutorService,
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
          public <T> QueryRunner<T> getQueryRunner(DruidServer server)
          {
            return serverView.getQueryRunner(server);
          }

          @Override
          public void registerServerCallback(Executor exec, ServerCallback callback)
          {

          }
        },
        cache,
        jsonMapper,
        backgroundExecutorService,
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
        }
    );
  }

  private static class ServerExpectation<T>
  {
    private final String segmentId;
    private final Interval interval;
    private final DataSegment segment;
    private final Iterable<Result<T>> results;

    public ServerExpectation(
        String segmentId,
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

    public String getSegmentId()
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
            new Interval(0, 1),
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
        } catch (IllegalStateException e) {
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
      public String getIdentifier()
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
    private final List<ServerExpectation> expectations = Lists.newArrayList();

    public ServerExpectations(
        DruidServer server,
        QueryRunner queryRunner
    )
    {
      this.server = server;
      this.queryRunner = queryRunner;
    }

    public DruidServer getServer()
    {
      return server;
    }

    public QueryRunner getQueryRunner()
    {
      return queryRunner;
    }

    public List<ServerExpectation> getExpectations()
    {
      return expectations;
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
  public void testTimeBoundaryCachingWhenTimeIsInteger() throws Exception
  {
    testQueryCaching(
        client,
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .build(),
        new Interval("1970-01-01/1970-01-02"),
        makeTimeBoundaryResult(new DateTime("1970-01-01"), new DateTime("1970-01-01"), new DateTime("1970-01-02")),

        new Interval("1970-01-01/2011-01-03"),
        makeTimeBoundaryResult(new DateTime("1970-01-02"), new DateTime("1970-01-02"), new DateTime("1970-01-03")),

        new Interval("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("1970-01-05"), new DateTime("1970-01-05"), new DateTime("1970-01-10")),

        new Interval("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("1970-01-05T01"), new DateTime("1970-01-05T01"), new DateTime("1970-01-10"))
    );

    testQueryCaching(
        client,
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .bound(TimeBoundaryQuery.MAX_TIME)
              .build(),
        new Interval("1970-01-01/2011-01-02"),
        makeTimeBoundaryResult(new DateTime("1970-01-01"), null, new DateTime("1970-01-02")),

        new Interval("1970-01-01/2011-01-03"),
        makeTimeBoundaryResult(new DateTime("1970-01-02"), null, new DateTime("1970-01-03")),

        new Interval("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("1970-01-05"), null, new DateTime("1970-01-10")),

        new Interval("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("1970-01-05T01"), null, new DateTime("1970-01-10"))
    );

    testQueryCaching(
        client,
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(CachingClusteredClientTest.DATA_SOURCE)
              .intervals(CachingClusteredClientTest.SEG_SPEC)
              .context(CachingClusteredClientTest.CONTEXT)
              .bound(TimeBoundaryQuery.MIN_TIME)
              .build(),
        new Interval("1970-01-01/2011-01-02"),
        makeTimeBoundaryResult(new DateTime("1970-01-01"), new DateTime("1970-01-01"), null),

        new Interval("1970-01-01/2011-01-03"),
        makeTimeBoundaryResult(new DateTime("1970-01-02"), new DateTime("1970-01-02"), null),

        new Interval("1970-01-01/1970-01-10"),
        makeTimeBoundaryResult(new DateTime("1970-01-05"), new DateTime("1970-01-05"), null),

        new Interval("1970-01-01/2011-01-10"),
        makeTimeBoundaryResult(new DateTime("1970-01-05T01"), new DateTime("1970-01-05T01"), null)
    );
  }

  @Test
  public void testGroupByCachingRenamedAggs() throws Exception
  {
    GroupByQuery.Builder builder = new GroupByQuery.Builder()
        .setDataSource(DATA_SOURCE)
        .setQuerySegmentSpec(SEG_SPEC)
        .setDimFilter(DIM_FILTER)
        .setGranularity(GRANULARITY)
        .setDimensions(Arrays.<DimensionSpec>asList(new DefaultDimensionSpec("a", "output")))
        .setAggregatorSpecs(AGGS)
        .setContext(CONTEXT);

    testQueryCaching(
        client,
        builder.build(),
        new Interval("2011-01-01/2011-01-02"),
        makeGroupByResults(
            new DateTime("2011-01-01"),
            ImmutableMap.of("output", "a", "rows", 1, "imps", 1, "impers", 1)
        ),

        new Interval("2011-01-02/2011-01-03"),
        makeGroupByResults(
            new DateTime("2011-01-02"),
            ImmutableMap.of("output", "b", "rows", 2, "imps", 2, "impers", 2)
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeGroupByResults(
            new DateTime("2011-01-05"), ImmutableMap.of("output", "c", "rows", 3, "imps", 3, "impers", 3),
            new DateTime("2011-01-06"), ImmutableMap.of("output", "d", "rows", 4, "imps", 4, "impers", 4),
            new DateTime("2011-01-07"), ImmutableMap.of("output", "e", "rows", 5, "imps", 5, "impers", 5),
            new DateTime("2011-01-08"), ImmutableMap.of("output", "f", "rows", 6, "imps", 6, "impers", 6),
            new DateTime("2011-01-09"), ImmutableMap.of("output", "g", "rows", 7, "imps", 7, "impers", 7)
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeGroupByResults(
            new DateTime("2011-01-05T01"), ImmutableMap.of("output", "c", "rows", 3, "imps", 3, "impers", 3),
            new DateTime("2011-01-06T01"), ImmutableMap.of("output", "d", "rows", 4, "imps", 4, "impers", 4),
            new DateTime("2011-01-07T01"), ImmutableMap.of("output", "e", "rows", 5, "imps", 5, "impers", 5),
            new DateTime("2011-01-08T01"), ImmutableMap.of("output", "f", "rows", 6, "imps", 6, "impers", 6),
            new DateTime("2011-01-09T01"), ImmutableMap.of("output", "g", "rows", 7, "imps", 7, "impers", 7)
        )
    );

    QueryRunner runner = new FinalizeResultsQueryRunner(
        client,
        GroupByQueryRunnerTest.makeQueryRunnerFactory(new GroupByQueryConfig()).getToolchest()
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedObjects(
        makeGroupByResults(
            new DateTime("2011-01-05T"), ImmutableMap.of("output", "c", "rows", 3, "imps", 3, "impers", 3),
            new DateTime("2011-01-05T01"), ImmutableMap.of("output", "c", "rows", 3, "imps", 3, "impers", 3),
            new DateTime("2011-01-06T"), ImmutableMap.of("output", "d", "rows", 4, "imps", 4, "impers", 4),
            new DateTime("2011-01-06T01"), ImmutableMap.of("output", "d", "rows", 4, "imps", 4, "impers", 4),
            new DateTime("2011-01-07T"), ImmutableMap.of("output", "e", "rows", 5, "imps", 5, "impers", 5),
            new DateTime("2011-01-07T01"), ImmutableMap.of("output", "e", "rows", 5, "imps", 5, "impers", 5),
            new DateTime("2011-01-08T"), ImmutableMap.of("output", "f", "rows", 6, "imps", 6, "impers", 6),
            new DateTime("2011-01-08T01"), ImmutableMap.of("output", "f", "rows", 6, "imps", 6, "impers", 6),
            new DateTime("2011-01-09T"), ImmutableMap.of("output", "g", "rows", 7, "imps", 7, "impers", 7),
            new DateTime("2011-01-09T01"), ImmutableMap.of("output", "g", "rows", 7, "imps", 7, "impers", 7)
        ),
        runner.run(
            builder.setInterval("2011-01-05/2011-01-10")
                   .build(),
            context
        ),
        ""
    );

    TestHelper.assertExpectedObjects(
        makeGroupByResults(
            new DateTime("2011-01-05T"), ImmutableMap.of("output2", "c", "rows2", 3, "imps", 3, "impers2", 3),
            new DateTime("2011-01-05T01"), ImmutableMap.of("output2", "c", "rows2", 3, "imps", 3, "impers2", 3),
            new DateTime("2011-01-06T"), ImmutableMap.of("output2", "d", "rows2", 4, "imps", 4, "impers2", 4),
            new DateTime("2011-01-06T01"), ImmutableMap.of("output2", "d", "rows2", 4, "imps", 4, "impers2", 4),
            new DateTime("2011-01-07T"), ImmutableMap.of("output2", "e", "rows2", 5, "imps", 5, "impers2", 5),
            new DateTime("2011-01-07T01"), ImmutableMap.of("output2", "e", "rows2", 5, "imps", 5, "impers2", 5),
            new DateTime("2011-01-08T"), ImmutableMap.of("output2", "f", "rows2", 6, "imps", 6, "impers2", 6),
            new DateTime("2011-01-08T01"), ImmutableMap.of("output2", "f", "rows2", 6, "imps", 6, "impers2", 6),
            new DateTime("2011-01-09T"), ImmutableMap.of("output2", "g", "rows2", 7, "imps", 7, "impers2", 7),
            new DateTime("2011-01-09T01"), ImmutableMap.of("output2", "g", "rows2", 7, "imps", 7, "impers2", 7)
        ),
        runner.run(
            builder.setInterval("2011-01-05/2011-01-10")
                   .setDimensions(Arrays.<DimensionSpec>asList(new DefaultDimensionSpec("a", "output2")))
                   .setAggregatorSpecs(RENAMED_AGGS)
                   .build(),
            context
        ),
        "renamed aggregators test"
    );
  }

}
