/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.groupby.having.EqualToHavingSpec;
import io.druid.query.groupby.having.GreaterThanHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.having.OrHavingSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestHelper;
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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

@RunWith(Parameterized.class)
public class GroupByQueryRunnerTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;
  private Supplier<GroupByQueryConfig> configSupplier;

  @Before
  public void setUp() throws Exception
  {
    configSupplier = Suppliers.ofInstance(new GroupByQueryConfig());
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final StupidPool<ByteBuffer> pool = new StupidPool<ByteBuffer>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );

    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        configSupplier,
        new GroupByQueryQueryToolChest(configSupplier, mapper, engine)
    );

    GroupByQueryConfig singleThreadedConfig = new GroupByQueryConfig()
    {
      @Override
      public boolean isSingleThreaded()
      {
        return true;
      }
    };
    singleThreadedConfig.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> singleThreadedConfigSupplier = Suppliers.ofInstance(singleThreadedConfig);
    final GroupByQueryEngine singleThreadEngine = new GroupByQueryEngine(singleThreadedConfigSupplier, pool);

    final GroupByQueryRunnerFactory singleThreadFactory = new GroupByQueryRunnerFactory(
        singleThreadEngine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        singleThreadedConfigSupplier,
        new GroupByQueryQueryToolChest(singleThreadedConfigSupplier, mapper, singleThreadEngine)
    );


    Function<Object, Object> function = new Function<Object, Object>()
    {
      @Override
      public Object apply(@Nullable Object input)
      {
        return new Object[]{factory, ((Object[]) input)[0]};
      }
    };

    return Lists.newArrayList(
        Iterables.concat(
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(factory),
                function
            ),
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(singleThreadFactory),
                function
            )
        )
    );
  }

  public GroupByQueryRunnerTest(GroupByQueryRunnerFactory factory, QueryRunner runner)
  {
    this.factory = factory;
    this.runner = runner;
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithUniques()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.qualityUniques
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "uniques",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithCardinality()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.qualityCardinality
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "cardinality",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithDimExtractionFn()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new RegexDimExtractionFn("(\\w{1})")
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "a", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "b", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "e", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "h", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "m", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "n", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "p", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "t", "rows", 2L, "idx", 197L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "a", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "b", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "e", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "h", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "m", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "n", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "p", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "t", "rows", 2L, "idx", 223L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithTimeZone()
  {
    DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setInterval("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
                                     .setDimensions(
                                         Lists.newArrayList(
                                             (DimensionSpec) new DefaultDimensionSpec(
                                                 "quality",
                                                 "alias"
                                             )
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         Arrays.<AggregatorFactory>asList(
                                             QueryRunnerTestHelper.rowsCount,
                                             new LongSumAggregatorFactory(
                                                 "idx",
                                                 "index"
                                             )
                                         )
                                     )
                                     .setGranularity(
                                         new PeriodGranularity(
                                             new Period("P1D"),
                                             null,
                                             tz
                                         )
                                     )
                                     .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-03-31", tz), "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-03-31", tz), "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-03-31", tz), "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-03-31", tz), "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-03-31", tz), "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-03-31", tz), "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-03-31", tz), "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-03-31", tz), "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-03-31", tz), "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-04-01", tz), "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-04-01", tz), "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-04-01", tz), "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-04-01", tz), "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-04-01", tz), "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-04-01", tz), "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-04-01", tz), "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-04-01", tz), "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow(new DateTime("2011-04-01", tz), "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testMergeResults()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery fullQuery = builder.build();
    final GroupByQuery allGranQuery = builder.copy().setGranularity(QueryGranularity.ALL).build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence run(Query<Row> query)
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return Sequences.concat(runner.run(query1), runner.run(query2));
          }
        }
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    TestHelper.assertExpectedObjects(expectedResults, runner.run(fullQuery), "direct");
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery), "merged");

    List<Row> allGranExpectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    TestHelper.assertExpectedObjects(allGranExpectedResults, runner.run(allGranQuery), "direct");
    TestHelper.assertExpectedObjects(allGranExpectedResults, mergedRunner.run(allGranQuery), "merged");
  }

  @Test
  public void testMergeResultsWithLimit()
  {
    for (int limit = 1; limit < 20; ++limit) {
      doTestMergeResultsWithValidLimit(limit);
    }
  }

  private void doTestMergeResultsWithValidLimit(final int limit)
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimit(Integer.valueOf(limit));

    final GroupByQuery fullQuery = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit), mergeRunner.run(fullQuery), String.format("limit: %d", limit)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeResultsWithNegativeLimit()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimit(Integer.valueOf(-1));

    builder.build();
  }

  @Test
  public void testMergeResultsWithOrderBy()
  {
    LimitSpec[] orderBySpecs = new LimitSpec[]{
        new DefaultLimitSpec(OrderByColumnSpec.ascending("idx"), null),
        new DefaultLimitSpec(OrderByColumnSpec.ascending("rows", "idx"), null),
        new DefaultLimitSpec(OrderByColumnSpec.descending("idx"), null),
        new DefaultLimitSpec(OrderByColumnSpec.descending("rows", "idx"), null),
    };

    final Comparator<Row> idxComparator =
        new Comparator<Row>()
        {
          @Override
          public int compare(Row o1, Row o2)
          {
            return Float.compare(o1.getFloatMetric("idx"), o2.getFloatMetric("idx"));
          }
        };

    Comparator<Row> rowsIdxComparator =
        new Comparator<Row>()
        {

          @Override
          public int compare(Row o1, Row o2)
          {
            int value = Float.compare(o1.getFloatMetric("rows"), o2.getFloatMetric("rows"));
            if (value != 0) {
              return value;
            }

            return idxComparator.compare(o1, o2);
          }
        };

    List<Row> allResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    List<List<Row>> expectedResults = Lists.newArrayList(
        Ordering.from(idxComparator).sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).sortedCopy(allResults),
        Ordering.from(idxComparator).reverse().sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).reverse().sortedCopy(allResults)
    );

    for (int i = 0; i < orderBySpecs.length; ++i) {
      doTestMergeResultsWithOrderBy(orderBySpecs[i], expectedResults.get(i));
    }
  }

  private void doTestMergeResultsWithOrderBy(LimitSpec orderBySpec, List<Row> expectedResults)
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimitSpec(orderBySpec);

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence run(Query<Row> query)
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return Sequences.concat(runner.run(query1), runner.run(query2));
          }
        }
    );

    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery), "merged");
  }

  @Test
  public void testGroupByOrderLimit() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .addOrderByColumn("rows")
        .addOrderByColumn("alias", OrderByColumnSpec.Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query), "no-limit");

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), mergeRunner.run(builder.limit(5).build()), "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit2() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .addOrderByColumn("rows", "desc")
        .addOrderByColumn("alias", "d")
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), mergeRunner.run(builder.limit(5).build()), "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit3() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleSumAggregatorFactory("idx", "index")
            )
        )
        .addOrderByColumn("idx", "desc")
        .addOrderByColumn("alias", "d")
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4423.6533203125D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4418.61865234375D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319.94403076171875D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 270.3977966308594D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243.65843200683594D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 222.20980834960938D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 218.7224884033203D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216.97836303710938D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 178.24917602539062D)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), mergeRunner.run(builder.limit(5).build()), "limited"
    );
  }

  @Test
  public void testGroupByWithMixedCasingOrdering()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.providerDimension,
                    "ProviderAlias"
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "providerALIAS",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "provideralias", "upfront", "rows", 186L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "provideralias", "total_market", "rows", 186L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "provideralias", "spot", "rows", 837L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithOrderLimit4()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.providerDimension,
                    QueryRunnerTestHelper.providerDimension
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.providerDimension,
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "provider", "upfront", "rows", 186L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "provider", "total_market", "rows", 186L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "provider", "spot", "rows", 837L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec("rows", 2L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence run(Query<Row> query)
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return Sequences.concat(runner.run(query1), runner.run(query2));
          }
        }
    );

    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery), "merged");
  }

  @Test
  public void testGroupByWithRegEx() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimFilter(new RegexDimFilter("quality", "auto.*"))
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "quality")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "rows", 2L)
    );

    final GroupByQueryEngine engine = new GroupByQueryEngine(
        configSupplier,
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(1024 * 1024);
              }
            }
        )
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest(
        configSupplier,
        new DefaultObjectMapper(),
        engine
    ).mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query), "no-limit");
  }

  @Test
  public void testGroupByWithMetricColumnDisappears() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .addDimension("quality")
        .addDimension("index")
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "business", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "entertainment", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "health", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "mezzanine", "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "news", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "premium", "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "technology", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "travel", "rows", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, runner.run(query), "normal");
    final GroupByQueryEngine engine = new GroupByQueryEngine(
        configSupplier,
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(1024 * 1024);
              }
            }
        )
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest(
        configSupplier,
        new DefaultObjectMapper(),
        engine
    ).mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query), "no-limit");
  }

  @Test
  public void testGroupByWithNonexistentDimension() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .addDimension("billy")
        .addDimension("quality")
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "business", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "entertainment", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "health", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "mezzanine", "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "news", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "premium", "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "technology", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "travel", "rows", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, runner.run(query), "normal");
    final GroupByQueryEngine engine = new GroupByQueryEngine(
        configSupplier,
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(1024 * 1024);
              }
            }
        )
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest(
        configSupplier,
        new DefaultObjectMapper(),
        engine
    ).mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query), "no-limit");
  }

  // A subquery identical to the query should yield identical results
  @Test
  public void testIdenticalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(new JavaScriptDimFilter("quality", "function(dim){ return true; }"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testDifferentGroupingSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new MaxAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "idx", 2900.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx", 2505.0)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testDifferentIntervalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.secondOnly)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new MaxAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx", 2505.0)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testEmptySubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.emptyInterval)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new MaxAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Assert.assertFalse(results.iterator().hasNext());
  }

  @Test
  public void testSubqueryWithPostAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(new JavaScriptDimFilter("quality", "function(dim){ return true; }"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx_subagg", "index")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg", "+", Arrays.<PostAggregator>asList(
                    new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                    new ConstantPostAggregator("thousand", 1000, 1000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx_subpostagg")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000, 10000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 11135.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 11118.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 11158.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 11120.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 13870.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 11121.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 13900.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 11078.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 11119.0),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 11147.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 11112.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 11166.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 11113.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 13447.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 11114.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 13505.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 11097.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 11126.0)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithPostAggregatorsAndHaving()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(new JavaScriptDimFilter("quality", "function(dim){ return true; }"))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx_subagg", "index")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                        new ConstantPostAggregator("thousand", 1000, 1000)
                    )
                )

            )
        )
        .setHavingSpec(
            new HavingSpec()
            {
              @Override
              public boolean eval(Row row)
              {
                return (row.getFloatMetric("idx_subpostagg") < 3800);
              }

              @Override
              public byte[] getCacheKey()
              {
                return new byte[0];
              }
            }
        )
        .addOrderByColumn("alias")
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx_subpostagg")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000, 10000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 11135.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 11118.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 11158.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 11120.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 11121.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 11078.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 11119.0),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 11147.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 11112.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 11166.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 11113.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 13447.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 11114.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 13505.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 11097.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 11126.0)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithMultiColumnAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setDimFilter(new JavaScriptDimFilter("provider", "function(dim){ return true; }"))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleSumAggregatorFactory("idx_subagg", "index"),
                new JavaScriptAggregatorFactory(
                    "js_agg",
                    Arrays.asList("index", "provider"),
                    "function(current, index, dim){return current + index + dim.length;}",
                    "function(){return 0;}",
                    "function(a,b){return a + b;}"
                )
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                        new ConstantPostAggregator("thousand", 1000, 1000)
                    )
                )

            )
        )
        .setHavingSpec(
            new HavingSpec()
            {
              @Override
              public boolean eval(Row row)
              {
                return (row.getFloatMetric("idx_subpostagg") < 3800);
              }

              @Override
              public byte[] getCacheKey()
              {
                return new byte[0];
              }
            }
        )
        .addOrderByColumn("alias")
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx_subpostagg"),
                new DoubleSumAggregatorFactory("js_outer_agg", "js_agg")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000, 10000)
                )
                )

            )
        )
        .setLimitSpec(
            new DefaultLimitSpec(
                Arrays.asList(
                    new OrderByColumnSpec(
                        "alias",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ),
                5
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            11119.0,
            "js_outer_agg",
            123.92274475097656
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            11078.0,
            "js_outer_agg",
            82.62254333496094
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            11121.0,
            "js_outer_agg",
            125.58358001708984
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            11120.0,
            "js_outer_agg",
            124.13470458984375
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            11158.0,
            "js_outer_agg",
            162.74722290039062
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithHyperUniques()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new HyperUniquesAggregatorFactory("quality_uniques", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx"),
                new HyperUniquesAggregatorFactory("uniq", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 282L, "uniq", 1.0002442201269182),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 230L, "uniq", 1.0002442201269182),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 324L, "uniq", 1.0002442201269182),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 233L, "uniq", 1.0002442201269182),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 5317L, "uniq", 1.0002442201269182),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 235L, "uniq", 1.0002442201269182),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 5405L, "uniq", 1.0002442201269182),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 175L, "uniq", 1.0002442201269182),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 245L, "uniq", 1.0002442201269182)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
