/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.MergeIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.TrinaryFn;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.RandomServerSelectorStrategy;
import io.druid.client.selector.ServerSelector;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.InsensitiveContainsSearchQuerySpec;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
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
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import io.druid.timeline.partition.StringPartitionChunk;
import org.easymock.Capture;
import org.easymock.EasyMock;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Executor;

/**
 */
@RunWith(Parameterized.class)
public class CachingClusteredClientTest
{
  public static final ImmutableMap<String, Object> CONTEXT = ImmutableMap.<String, Object>of("finalize", false);

  public static final MultipleIntervalSegmentSpec SEG_SPEC = new MultipleIntervalSegmentSpec(ImmutableList.<Interval>of());
  public static final String DATA_SOURCE = "test";
  protected static final DefaultObjectMapper jsonMapper = new DefaultObjectMapper(new SmileFactory());

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
              new ConstantPostAggregator("constant", 2, 2 )
          )
      ),
      new ArithmeticPostAggregator(
          "avg_imps_per_row_half",
          "/",
          Arrays.<PostAggregator>asList(
              new FieldAccessPostAggregator("avg_imps_per_row", "avg_imps_per_row"),
              new ConstantPostAggregator("constant", 2, 2 )
          )
      )
  );
  private static final List<AggregatorFactory> RENAMED_AGGS = Arrays.asList(
      new CountAggregatorFactory("rows2"),
      new LongSumAggregatorFactory("imps", "imps"),
      new LongSumAggregatorFactory("impers2", "imps")
  );
  private static final DimFilter DIM_FILTER = null;
  private static final List<PostAggregator> RENAMED_POST_AGGS = Arrays.asList();
  private static final QueryGranularity GRANULARITY = QueryGranularity.DAY;
  private static final DateTimeZone TIMEZONE = DateTimeZone.forID("America/Los_Angeles");
  private static final QueryGranularity PT1H_TZ_GRANULARITY = new PeriodGranularity(new Period("PT1H"), null, TIMEZONE);
  private static final String TOP_DIM = "a_dim";
  private final Random random;
  protected VersionedIntervalTimeline<String, ServerSelector> timeline;
  protected TimelineServerView serverView;
  protected Cache cache;
  public CachingClusteredClient client;
  DruidServer[] servers;

  public CachingClusteredClientTest(int randomSeed)
  {
    this.random = new Random(randomSeed);
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return Lists.transform(
        Lists.newArrayList(new RangeIterable(RANDOMNESS)),
        new Function<Integer, Object>()
        {
          @Override
          public Object apply(@Nullable Integer input)
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
    serverView = EasyMock.createStrictMock(TimelineServerView.class);
    cache = MapCache.create(100000);

    client = makeClient();

    servers = new DruidServer[]{
        new DruidServer("test1", "test1", 10, "historical", "bye", 0),
        new DruidServer("test2", "test2", 10, "historical", "bye", 0),
        new DruidServer("test3", "test3", 10, "historical", "bye", 0),
        new DruidServer("test4", "test4", 10, "historical", "bye", 0),
        new DruidServer("test5", "test5", 10, "historical", "bye", 0)
    };
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

    QueryRunner runner = new FinalizeResultsQueryRunner(client, new TimeseriesQueryQueryToolChest(new QueryConfig()));

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
                   .build()
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

    QueryRunner runner = new FinalizeResultsQueryRunner(client, new TimeseriesQueryQueryToolChest(new QueryConfig()));

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
                   .build()
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
    QueryRunner runner = new FinalizeResultsQueryRunner(client, new TimeseriesQueryQueryToolChest(new QueryConfig()));
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

    QueryRunner runner = new FinalizeResultsQueryRunner(client, new TopNQueryQueryToolChest(new TopNQueryConfig()));

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
                   .build()
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

    QueryRunner runner = new FinalizeResultsQueryRunner(client, new TopNQueryQueryToolChest(new TopNQueryConfig()));

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
                   .build()
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

    QueryRunner runner = new FinalizeResultsQueryRunner(client, new TopNQueryQueryToolChest(new TopNQueryConfig()));
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
                   .build()
        )
    );
  }

  @Test
  public  void testTopNOnPostAggMetricCaching() {
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

    QueryRunner runner = new FinalizeResultsQueryRunner(client, new TopNQueryQueryToolChest(new TopNQueryConfig()));
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
                   .build()
        )
    );
  }

  @Test
  public void testSearchCaching() throws Exception
  {
    testQueryCaching(
        client,
        new SearchQuery(
            new TableDataSource(DATA_SOURCE),
            DIM_FILTER,
            GRANULARITY,
            1000,
            SEG_SPEC,
            Arrays.asList("a_dim"),
            new InsensitiveContainsSearchQuerySpec("how"),
            null,
            CONTEXT
        ),
        new Interval("2011-01-01/2011-01-02"),
        makeSearchResults(new DateTime("2011-01-01"), "how", "howdy", "howwwwww", "howwy"),

        new Interval("2011-01-02/2011-01-03"),
        makeSearchResults(new DateTime("2011-01-02"), "how1", "howdy1", "howwwwww1", "howwy1"),

        new Interval("2011-01-05/2011-01-10"),
        makeSearchResults(
            new DateTime("2011-01-05"), "how2", "howdy2", "howwwwww2", "howww2",
            new DateTime("2011-01-06"), "how3", "howdy3", "howwwwww3", "howww3",
            new DateTime("2011-01-07"), "how4", "howdy4", "howwwwww4", "howww4",
            new DateTime("2011-01-08"), "how5", "howdy5", "howwwwww5", "howww5",
            new DateTime("2011-01-09"), "how6", "howdy6", "howwwwww6", "howww6"
        ),

        new Interval("2011-01-05/2011-01-10"),
        makeSearchResults(
            new DateTime("2011-01-05T01"), "how2", "howdy2", "howwwwww2", "howww2",
            new DateTime("2011-01-06T01"), "how3", "howdy3", "howwwwww3", "howww3",
            new DateTime("2011-01-07T01"), "how4", "howdy4", "howwwwww4", "howww4",
            new DateTime("2011-01-08T01"), "how5", "howdy5", "howwwwww5", "howww5",
            new DateTime("2011-01-09T01"), "how6", "howdy6", "howwwwww6", "howww6"
        )
    );
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
      final Query query, Object... args // does this assume query intervals must be ordered?
  )
  {
    if (args.length % 2 != 0) {
      throw new ISE("args.length must be divisible by two, was %d", args.length);
    }

    final List<Interval> queryIntervals = Lists.newArrayListWithCapacity(args.length / 2);
    final List<List<Iterable<Result<Object>>>> expectedResults = Lists.newArrayListWithCapacity(queryIntervals.size());

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

          EasyMock.expect(queryable.run(EasyMock.capture(capture)))
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
          EasyMock.expect(queryable.run(EasyMock.capture(capture)))
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
          EasyMock.expect(queryable.run(EasyMock.capture(capture)))
                  .andReturn(toQueryableSearchResults(segmentIds, intervals, results))
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
          EasyMock.expect(queryable.run(EasyMock.capture(capture)))
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
                                Arrays.asList(
                                    actualQueryInterval
                                )
                            )
                        )
                    )
                );
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

        ServerExpectation expectation = new ServerExpectation(
            String.format("%s_%s", k, j), // interval/chunk
            queryIntervals.get(numQueryIntervals),
            makeMock(mocks, DataSegment.class),
            expectedResults.get(k).get(j)
        );
        serverExpectations.get(lastServer).addExpectation(expectation);

        ServerSelector selector = new ServerSelector(expectation.getSegment(), new RandomServerSelectorStrategy());
        selector.addServer(new QueryableDruidServer(lastServer, null));

        final PartitionChunk<ServerSelector> chunk;
        if (numChunks == 1) {
          chunk = new SingleElementPartitionChunk<>(selector);
        } else {
          String start = null;
          String end = null;
          if (j > 0) {
            start = String.valueOf(j - 1);
          }
          if (j + 1 < numChunks) {
            end = String.valueOf(j);
          }
          chunk = new StringPartitionChunk<>(start, end, j, selector);
        }
        timeline.add(queryIntervals.get(k), String.valueOf(k), chunk);
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
                      .put("avg_imps_per_row",avg_impr)
                      .put("avg_imps_per_row_half",avg_impr / 2)
                      .put("avg_imps_per_row_double",avg_impr * 2)
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
      (Object... objects)
  {
    List<Result<SearchResultValue>> retVal = Lists.newArrayList();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List values = Lists.newArrayList();
      while (index < objects.length && !(objects[index] instanceof DateTime)) {
        values.add(new SearchHit(TOP_DIM, objects[index++].toString()));
      }

      retVal.add(new Result<>(timestamp, new SearchResultValue(values)));
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

  protected CachingClusteredClient makeClient()
  {
    return new CachingClusteredClient(
        new MapQueryToolChestWarehouse(
            ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                        .put(
                            TimeseriesQuery.class,
                            new TimeseriesQueryQueryToolChest(new QueryConfig())
                        )
                        .put(TopNQuery.class, new TopNQueryQueryToolChest(new TopNQueryConfig()))
                        .put(SearchQuery.class, new SearchQueryQueryToolChest(new SearchQueryConfig()))
                        .build()
        ),
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
        new CacheConfig()
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
            new NoneShardSpec(),
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
        return baseSegment.getShardSpec();
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
}
