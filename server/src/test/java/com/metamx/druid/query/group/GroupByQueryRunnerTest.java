package com.metamx.druid.query.group;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
import com.metamx.druid.query.segment.MultipleIntervalSegmentSpec;
import org.joda.time.DateTime;
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
import java.util.List;

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
    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(
                                         Lists.newArrayList(
                                             (DimensionSpec)new DefaultDimensionSpec(
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
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .build();

      List<Row> expectedResults = Arrays.asList(
          (Row) new MapBasedRow(new DateTime("2011-04-01"),ImmutableMap.<String, Object>of("alias", "automotive", "rows", 1L, "idx", 135L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01"),ImmutableMap.<String, Object>of("alias", "business",   "rows", 1L, "idx", 118L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01"),ImmutableMap.<String, Object>of("alias", "entertainment", "rows", 1L, "idx", 158L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01"),ImmutableMap.<String, Object>of("alias", "health", "rows", 1L, "idx", 120L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01"),ImmutableMap.<String, Object>of("alias", "mezzanine", "rows", 3L, "idx", 2870L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01"),ImmutableMap.<String, Object>of("alias", "news", "rows", 1L, "idx", 121L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01"),ImmutableMap.<String, Object>of("alias", "premium", "rows", 3L, "idx", 2900L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01"),ImmutableMap.<String, Object>of("alias", "technology", "rows", 1L, "idx", 78L)),
          (Row) new MapBasedRow(new DateTime("2011-04-01"),ImmutableMap.<String, Object>of("alias", "travel", "rows", 1L, "idx", 119L)),

          (Row) new MapBasedRow(new DateTime("2011-04-02"),ImmutableMap.<String, Object>of("alias", "automotive", "rows", 1L, "idx", 147L)),
          (Row) new MapBasedRow(new DateTime("2011-04-02"),ImmutableMap.<String, Object>of("alias", "business",   "rows", 1L, "idx", 112L)),
          (Row) new MapBasedRow(new DateTime("2011-04-02"),ImmutableMap.<String, Object>of("alias", "entertainment", "rows", 1L, "idx", 166L)),
          (Row) new MapBasedRow(new DateTime("2011-04-02"),ImmutableMap.<String, Object>of("alias", "health", "rows", 1L, "idx", 113L)),
          (Row) new MapBasedRow(new DateTime("2011-04-02"),ImmutableMap.<String, Object>of("alias", "mezzanine", "rows", 3L, "idx", 2447L)),
          (Row) new MapBasedRow(new DateTime("2011-04-02"),ImmutableMap.<String, Object>of("alias", "news", "rows", 1L, "idx", 114L)),
          (Row) new MapBasedRow(new DateTime("2011-04-02"),ImmutableMap.<String, Object>of("alias", "premium", "rows", 3L, "idx", 2505L)),
          (Row) new MapBasedRow(new DateTime("2011-04-02"),ImmutableMap.<String, Object>of("alias", "technology", "rows", 1L, "idx", 97L)),
          (Row) new MapBasedRow(new DateTime("2011-04-02"),ImmutableMap.<String, Object>of("alias", "travel", "rows", 1L, "idx", 126L))
      );

      Iterable<Row> results = Sequences.toList(
          runner.run(query),
          Lists.<Row>newArrayList()
      );

      TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testMergeResults() {
    GroupByQuery.Builder builder = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setInterval("2011-04-02/2011-04-04")
                                     .setDimensions(
                                         Lists.newArrayList(
                                             (DimensionSpec)new DefaultDimensionSpec(
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
                                     .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = new GroupByQueryQueryToolChest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence run(Query<Row> query)
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03"))));
            final Query query2 = query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04"))));
            return Sequences.<Row>concat(runner.run(query1), runner.run(query2));
          }
        }
    );

    List<Row> expectedResults = Arrays.asList(
        (Row) new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("alias", "automotive", "rows", 1L, "idx", 269L)
        ),
        (Row) new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("alias", "business", "rows", 1L, "idx", 217L)
        ),
        (Row) new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("alias", "entertainment", "rows", 1L, "idx", 319L)
        ),
        (Row) new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("alias", "health", "rows", 1L, "idx", 216L)
        ),
        (Row) new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("alias", "mezzanine", "rows", 3L, "idx", 4420L)
        ),
        (Row) new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("alias", "news", "rows", 1L, "idx", 221L)
        ),
        (Row) new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("alias", "premium", "rows", 3L, "idx", 4416L)
        ),
        (Row) new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("alias", "technology", "rows", 1L, "idx", 177L)
        ),
        (Row) new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("alias", "travel", "rows", 1L, "idx", 243L)
        )
    );

    Iterable<Row> results = Sequences.toList(
        mergedRunner.run(fullQuery),
        Lists.<Row>newArrayList()
    );

    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
