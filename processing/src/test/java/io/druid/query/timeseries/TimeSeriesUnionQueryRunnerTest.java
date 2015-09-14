/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.timeseries;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Query;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
            new TimeseriesQueryEngine(),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        QueryRunnerTestHelper.unionDataSource
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query, context),
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
    QueryToolChest toolChest = new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator());
    QueryRunner mergingrunner = toolChest.mergeResults(
        new UnionQueryRunner<Result<TimeseriesResultValue>>(
            new QueryRunner<Result<TimeseriesResultValue>>()
            {
              @Override
              public Sequence<Result<TimeseriesResultValue>> run(Query<Result<TimeseriesResultValue>> query,
                                                                 Map<String, Object> responseContext
              )
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
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 5L, "idx", 6L)
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 8L, "idx", 10L)
            )
        ),
        new Result<>(
            new DateTime("2011-04-03"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 3L, "idx", 4L)
            )
        ),
        new Result<>(
            new DateTime("2011-04-04"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 9L, "idx", 10L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        mergingrunner.run(query, Maps.<String, Object>newHashMap()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    System.out.println(results);
    TestHelper.assertExpectedResults(expectedResults, results);

  }

}
