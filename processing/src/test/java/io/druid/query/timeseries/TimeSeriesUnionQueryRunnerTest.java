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

package io.druid.query.timeseries;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.UnionDataSource;
import io.druid.query.UnionQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class TimeSeriesUnionQueryRunnerTest
{
  private final QueryRunner runner;

  public TimeSeriesUnionQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.makeUnionQueryRunners(
        new TimeseriesQueryRunnerFactory(
            new TimeseriesQueryQueryToolChest(new QueryConfig()),
            new TimeseriesQueryEngine(),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    );
  }

  @Test
  public void testUnionTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.unionDataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          ),
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 52L, "idx", 26476L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 52L, "idx", 23308L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testUnionResultMerging()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(
                                      new UnionDataSource(
                                          Lists.newArrayList(
                                              new TableDataSource("ds1"),
                                              new TableDataSource("ds2")
                                          )
                                      )
                                  )
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .build();
    QueryToolChest toolChest = new TimeseriesQueryQueryToolChest(new QueryConfig());
    QueryRunner mergingrunner = toolChest.mergeResults(
        new UnionQueryRunner<Result<TimeseriesResultValue>>(
            new QueryRunner<Result<TimeseriesResultValue>>()
            {
              @Override
              public Sequence<Result<TimeseriesResultValue>> run(Query<Result<TimeseriesResultValue>> query)
              {
                if (query.getDataSource().equals(new TableDataSource("ds1"))) {
                  return Sequences.simple(
                      Lists.newArrayList(
                          new Result<TimeseriesResultValue>(
                              new DateTime("2011-04-02"),
                              new TimeseriesResultValue(
                                  ImmutableMap.<String, Object>of(
                                      "rows",
                                      1L,
                                      "idx",
                                      2L
                                  )
                              )
                          ),
                          new Result<TimeseriesResultValue>(
                              new DateTime("2011-04-03"),
                              new TimeseriesResultValue(
                                  ImmutableMap.<String, Object>of(
                                      "rows",
                                      3L,
                                      "idx",
                                      4L
                                  )
                              )
                          )
                      )
                  );
                } else {
                  return Sequences.simple(
                      Lists.newArrayList(
                          new Result<TimeseriesResultValue>(
                              new DateTime("2011-04-01"),
                              new TimeseriesResultValue(
                                  ImmutableMap.<String, Object>of(
                                      "rows",
                                      5L,
                                      "idx",
                                      6L
                                  )
                              )
                          ),
                          new Result<TimeseriesResultValue>(
                              new DateTime("2011-04-02"),
                              new TimeseriesResultValue(
                                  ImmutableMap.<String, Object>of(
                                      "rows",
                                      7L,
                                      "idx",
                                      8L
                                  )
                              )
                          ),
                          new Result<TimeseriesResultValue>(
                              new DateTime("2011-04-04"),
                              new TimeseriesResultValue(
                                  ImmutableMap.<String, Object>of(
                                      "rows",
                                      9L,
                                      "idx",
                                      10L
                                  )
                              )
                          )
                      )
                  );
                }
              }
            },
            toolChest
        )
    );

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 5L, "idx", 6L)
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 8L, "idx", 10L)
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-03"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 3L, "idx", 4L)
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-04"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 9L, "idx", 10L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        mergingrunner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    System.out.println(results);
    TestHelper.assertExpectedResults(expectedResults, results);

  }

}
