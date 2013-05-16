/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.group;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.PeriodGranularity;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.TestHelper;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.DoubleSumAggregatorFactory;
import com.metamx.druid.aggregation.LongSumAggregatorFactory;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.input.MapBasedRow;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerTestHelper;
import com.metamx.druid.query.dimension.DefaultDimensionSpec;
import com.metamx.druid.query.dimension.DimensionSpec;
import com.metamx.druid.query.filter.RegexDimFilter;
import com.metamx.druid.query.group.having.EqualToHavingSpec;
import com.metamx.druid.query.group.having.GreaterThanHavingSpec;
import com.metamx.druid.query.group.having.OrHavingSpec;
import com.metamx.druid.query.group.orderby.DefaultLimitSpec;
import com.metamx.druid.query.group.orderby.LimitSpec;
import com.metamx.druid.query.group.orderby.OrderByColumnSpec;
import com.metamx.druid.query.segment.MultipleIntervalSegmentSpec;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
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
import java.util.Map;

@RunWith(Parameterized.class)
public class GroupByQueryRunnerTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        new GroupByQueryEngine(
            new GroupByQueryEngineConfig()
            {
              @Override
              public int getMaxIntermediateRows()
              {
                return 10000;
              }
            },
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
        ),
        new GroupByQueryRunnerFactoryConfig()
        {
        }
    );

    return Lists.newArrayList(
        Iterables.transform(
            QueryRunnerTestHelper.makeQueryRunners(factory), new Function<Object, Object>()
        {
          @Override
          public Object apply(@Nullable Object input)
          {
            return new Object[]{factory, ((Object[]) input)[0]};
          }
        }
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
        createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = Sequences.toList(runner.run(query), Lists.<Row>newArrayList());

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
        createExpectedRow(new DateTime("2011-03-31", tz), "alias", "automotive", "rows", 1L, "idx", 135L),
        createExpectedRow(new DateTime("2011-03-31", tz), "alias", "business", "rows", 1L, "idx", 118L),
        createExpectedRow(new DateTime("2011-03-31", tz), "alias", "entertainment", "rows", 1L, "idx", 158L),
        createExpectedRow(new DateTime("2011-03-31", tz), "alias", "health", "rows", 1L, "idx", 120L),
        createExpectedRow(new DateTime("2011-03-31", tz), "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        createExpectedRow(new DateTime("2011-03-31", tz), "alias", "news", "rows", 1L, "idx", 121L),
        createExpectedRow(new DateTime("2011-03-31", tz), "alias", "premium", "rows", 3L, "idx", 2900L),
        createExpectedRow(new DateTime("2011-03-31", tz), "alias", "technology", "rows", 1L, "idx", 78L),
        createExpectedRow(new DateTime("2011-03-31", tz), "alias", "travel", "rows", 1L, "idx", 119L),

        createExpectedRow(new DateTime("2011-04-01", tz), "alias", "automotive", "rows", 1L, "idx", 147L),
        createExpectedRow(new DateTime("2011-04-01", tz), "alias", "business", "rows", 1L, "idx", 112L),
        createExpectedRow(new DateTime("2011-04-01", tz), "alias", "entertainment", "rows", 1L, "idx", 166L),
        createExpectedRow(new DateTime("2011-04-01", tz), "alias", "health", "rows", 1L, "idx", 113L),
        createExpectedRow(new DateTime("2011-04-01", tz), "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        createExpectedRow(new DateTime("2011-04-01", tz), "alias", "news", "rows", 1L, "idx", 114L),
        createExpectedRow(new DateTime("2011-04-01", tz), "alias", "premium", "rows", 3L, "idx", 2505L),
        createExpectedRow(new DateTime("2011-04-01", tz), "alias", "technology", "rows", 1L, "idx", 97L),
        createExpectedRow(new DateTime("2011-04-01", tz), "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = Sequences.toList(
        runner.run(query),
        Lists.<Row>newArrayList()
    );

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

    QueryRunner mergedRunner = new GroupByQueryQueryToolChest().mergeResults(
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
        createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    TestHelper.assertExpectedObjects(expectedResults, runner.run(fullQuery), "direct");
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery), "merged");

    List<Row> allGranExpectedResults = Arrays.asList(
        createExpectedRow("2011-04-02", "alias", "automotive", "rows", 2L, "idx", 269L),
        createExpectedRow("2011-04-02", "alias", "business", "rows", 2L, "idx", 217L),
        createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 2L, "idx", 319L),
        createExpectedRow("2011-04-02", "alias", "health", "rows", 2L, "idx", 216L),
        createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        createExpectedRow("2011-04-02", "alias", "news", "rows", 2L, "idx", 221L),
        createExpectedRow("2011-04-02", "alias", "premium", "rows", 6L, "idx", 4416L),
        createExpectedRow("2011-04-02", "alias", "technology", "rows", 2L, "idx", 177L),
        createExpectedRow("2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L)
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
        createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest().mergeResults(runner);

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
        createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
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

    QueryRunner mergedRunner = new GroupByQueryQueryToolChest().mergeResults(
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
        createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L)
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest().mergeResults(runner);
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
        createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L)
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest().mergeResults(runner);
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
        createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4423.6533203125D),
        createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4418.61865234375D),
        createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319.94403076171875D),
        createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 270.3977966308594D),
        createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243.65843200683594D),
        createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 222.20980834960938D),
        createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 218.7224884033203D),
        createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216.97836303710938D),
        createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 178.24917602539062D)
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), mergeRunner.run(builder.limit(5).build()), "limited"
    );
  }

  @Test
  public void testHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
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

    QueryRunner mergedRunner = new GroupByQueryQueryToolChest().mergeResults(
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
        createExpectedRow("2011-04-01", "quality", "automotive", "rows", 2L)
  );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query), "no-limit");
  }

  private Row createExpectedRow(final String timestamp, Object... vals)
  {
    return createExpectedRow(new DateTime(timestamp), vals);
  }

  private Row createExpectedRow(final DateTime timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    Map<String, Object> theVals = Maps.newHashMap();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }

    return new MapBasedRow(new DateTime(timestamp), theVals);
  }
}
