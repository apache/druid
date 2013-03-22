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
import com.google.common.collect.*;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.PeriodGranularity;
import com.metamx.druid.Query;
import com.metamx.druid.TestHelper;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.LongSumAggregatorFactory;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.input.MapBasedRow;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerTestHelper;
import com.metamx.druid.query.dimension.DefaultDimensionSpec;
import com.metamx.druid.query.dimension.DimensionSpec;
import com.metamx.druid.query.having.EqualToHavingSpec;
import com.metamx.druid.query.having.GreaterThanHavingSpec;
import com.metamx.druid.query.having.OrHavingSpec;
import com.metamx.druid.query.order.AscendingOrderBySpec;
import com.metamx.druid.query.order.DescendingOrderBySpec;
import com.metamx.druid.query.order.OrderBySpec;
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
import java.util.*;

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
                    )
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

  public GroupByQueryRunnerTest(GroupByQueryRunnerFactory factory, QueryRunner runner) {
    this.factory = factory;
    this.runner = runner;
  }

  @Test
  public void testGroupBy() {
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

      List<Row> expectedResults = Arrays.<Row>asList(
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
  public void testGroupByWithTimeZone() {
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
          (Row) new MapBasedRow(new DateTime("2011-03-31", tz),ImmutableMap.<String, Object>of("alias", "automotive", "rows", 1L, "idx", 135L)),
          (Row) new MapBasedRow(new DateTime("2011-03-31", tz),ImmutableMap.<String, Object>of("alias", "business",   "rows", 1L, "idx", 118L)),
          (Row) new MapBasedRow(new DateTime("2011-03-31", tz),ImmutableMap.<String, Object>of("alias", "entertainment", "rows", 1L, "idx", 158L)),
          (Row) new MapBasedRow(new DateTime("2011-03-31", tz),ImmutableMap.<String, Object>of("alias", "health", "rows", 1L, "idx", 120L)),
          (Row) new MapBasedRow(new DateTime("2011-03-31", tz),ImmutableMap.<String, Object>of("alias", "mezzanine", "rows", 3L, "idx", 2870L)),
          (Row) new MapBasedRow(new DateTime("2011-03-31", tz),ImmutableMap.<String, Object>of("alias", "news", "rows", 1L, "idx", 121L)),
          (Row) new MapBasedRow(new DateTime("2011-03-31", tz),ImmutableMap.<String, Object>of("alias", "premium", "rows", 3L, "idx", 2900L)),
          (Row) new MapBasedRow(new DateTime("2011-03-31", tz),ImmutableMap.<String, Object>of("alias", "technology", "rows", 1L, "idx", 78L)),
          (Row) new MapBasedRow(new DateTime("2011-03-31", tz),ImmutableMap.<String, Object>of("alias", "travel", "rows", 1L, "idx", 119L)),

          (Row) new MapBasedRow(new DateTime("2011-04-01", tz),ImmutableMap.<String, Object>of("alias", "automotive", "rows", 1L, "idx", 147L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01", tz),ImmutableMap.<String, Object>of("alias", "business",   "rows", 1L, "idx", 112L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01", tz),ImmutableMap.<String, Object>of("alias", "entertainment", "rows", 1L, "idx", 166L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01", tz),ImmutableMap.<String, Object>of("alias", "health", "rows", 1L, "idx", 113L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01", tz),ImmutableMap.<String, Object>of("alias", "mezzanine", "rows", 3L, "idx", 2447L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01", tz),ImmutableMap.<String, Object>of("alias", "news", "rows", 1L, "idx", 114L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01", tz),ImmutableMap.<String, Object>of("alias", "premium", "rows", 3L, "idx", 2505L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01", tz),ImmutableMap.<String, Object>of("alias", "technology", "rows", 1L, "idx", 97L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01", tz),ImmutableMap.<String, Object>of("alias", "travel", "rows", 1L, "idx", 126L))
      );

      Iterable<Row> results = Sequences.toList(
          runner.run(query),
          Lists.<Row>newArrayList()
      );

      TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testMergeResults() {
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

    List<Row> expectedResults = Arrays.<Row>asList(
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
  }

  @Test
  public void testMergeResultsWithLimit() {
    for(int limit = 0; limit < 20; ++limit) {
      doTestMergeResultsWithValidLimit(limit);
    }

  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyOrderByField() {
    doTestMergeResultsWithOrderBy(new AscendingOrderBySpec(new ArrayList<String>()), new ArrayList<Row>());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidOrderByField() {
    doTestMergeResultsWithOrderBy(new DescendingOrderBySpec(Lists.newArrayList("non-existing-aggregation-name")), new ArrayList<Row>());
  }
  private void doTestMergeResultsWithValidLimit(int limit)
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

    List<Row> expectedResults = Arrays.<Row>asList(
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
    if(limit <= expectedResults.size()){
      TestHelper.assertExpectedObjects(expectedResults.subList(0, limit), mergedRunner.run(fullQuery), "merged");
    } else {
      TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery), "merged");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeResultsWithNegativeLimit() {
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

    List<Row> expectedResults = Arrays.<Row>asList(
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
    TestHelper.assertExpectedObjects(expectedResults.subList(0, 2), mergedRunner.run(fullQuery), "merged");
  }

  @Test
  public void testMergeResultsWithOrderBy() {
    OrderBySpec orderBySpecs[] = new OrderBySpec[]{
      new AscendingOrderBySpec(Lists.newArrayList("idx")),
      new AscendingOrderBySpec(Lists.newArrayList("rows", "idx")),
      new DescendingOrderBySpec(Lists.newArrayList("idx")),
      new DescendingOrderBySpec(Lists.newArrayList("rows", "idx")),
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
      new Comparator<Row>() {

        @Override
        public int compare(Row o1, Row o2)
        {
          int value = Float.compare(o1.getFloatMetric("rows"), o2.getFloatMetric("rows"));
          if(value != 0) {
            return value;
          }

          return idxComparator.compare(o1, o2);
        }
      };

    List<Row> allResults = Arrays.<Row>asList(
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

    for(int i = 0; i < orderBySpecs.length; ++i){
      doTestMergeResultsWithOrderBy(orderBySpecs[i], expectedResults.get(i));
    }
  }

  private void doTestMergeResultsWithOrderBy(OrderBySpec orderBySpec, List<Row> expectedResults)
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
      .setOrderBySpec(orderBySpec);

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

  private MapBasedRow createExpectedRow(final String timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    Map<String, Object> theVals = Maps.newHashMap();
    for (int i = 0; i < vals.length; i+=2) {
      theVals.put(vals[i].toString(), vals[i+1]);
    }

    return new MapBasedRow(new DateTime(timestamp), theVals);
  }

  @Test
  public void testHavingSpec() {
    List<Row> expectedResults = Arrays.<Row>asList(
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
      .setHavingSpec(new OrHavingSpec(ImmutableList.of(
          new GreaterThanHavingSpec("rows", 2L),
          new EqualToHavingSpec("idx", 217L)
        )
      ));

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
}
