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

package org.apache.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerTest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
@RunWith(Parameterized.class)
public class GroupByTimeseriesQueryRunnerTest extends TimeseriesQueryRunnerTest
{
  private static final Closer resourceCloser = Closer.create();

  @AfterClass
  public static void teardown() throws IOException
  {
    resourceCloser.close();
  }

  @SuppressWarnings("unchecked")
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);
    final Pair<GroupByQueryRunnerFactory, Closer> factoryAndCloser = GroupByQueryRunnerTest.makeQueryRunnerFactory(
        config
    );
    final GroupByQueryRunnerFactory factory = factoryAndCloser.lhs;
    resourceCloser.register(factoryAndCloser.rhs);
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        Lists.transform(
            QueryRunnerTestHelper.makeQueryRunners(factory),
            new Function<QueryRunner<Row>, Object>()
            {
              @Override
              public Object apply(final QueryRunner<Row> input)
              {
                return new QueryRunner()
                {
                  @Override
                  public Sequence run(QueryPlus queryPlus, Map responseContext)
                  {
                    TimeseriesQuery tsQuery = (TimeseriesQuery) queryPlus.getQuery();
                    QueryRunner<Row> newRunner = factory.mergeRunners(
                        Execs.directExecutor(), ImmutableList.of(input)
                    );
                    QueryToolChest toolChest = factory.getToolchest();

                    newRunner = new FinalizeResultsQueryRunner<>(
                        toolChest.mergeResults(toolChest.preMergeQueryDecoration(newRunner)),
                        toolChest
                    );

                    GroupByQuery newQuery = GroupByQuery
                        .builder()
                        .setDataSource(tsQuery.getDataSource())
                        .setQuerySegmentSpec(tsQuery.getQuerySegmentSpec())
                        .setGranularity(tsQuery.getGranularity())
                        .setDimFilter(tsQuery.getDimensionsFilter())
                        .setAggregatorSpecs(tsQuery.getAggregatorSpecs())
                        .setPostAggregatorSpecs(tsQuery.getPostAggregatorSpecs())
                        .setVirtualColumns(tsQuery.getVirtualColumns())
                        .setContext(tsQuery.getContext())
                        .build();

                    return Sequences.map(
                        newRunner.run(queryPlus.withQuery(newQuery), responseContext),
                        new Function<Row, Result<TimeseriesResultValue>>()
                        {
                          @Override
                          public Result<TimeseriesResultValue> apply(final Row input)
                          {
                            MapBasedRow row = (MapBasedRow) input;

                            return new Result<>(
                                row.getTimestamp(), new TimeseriesResultValue(row.getEvent())
                            );
                          }
                        }
                    );
                  }

                  @Override
                  public String toString()
                  {
                    return input.toString();
                  }
                };
              }
            }
        )
    );
  }

  public GroupByTimeseriesQueryRunnerTest(QueryRunner runner)
  {
    super(runner, false, QueryRunnerTestHelper.commonDoubleAggregators);
  }

  // GroupBy handles timestamps differently when granularity is ALL
  @Override
  @Test
  public void testFullOnTimeseriesMaxMin()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                  .aggregators(
                                      new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                      new DoubleMinAggregatorFactory("minIndex", "index")
                                  )
                                  .descending(descending)
                                  .build();

    DateTime expectedEarliest = DateTimes.of("1970-01-01");
    DateTime expectedLast = DateTimes.of("2011-04-15");

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    Result<TimeseriesResultValue> result = results.iterator().next();

    Assert.assertEquals(expectedEarliest, result.getTimestamp());
    Assert.assertFalse(
        StringUtils.format("Timestamp[%s] > expectedLast[%s]", result.getTimestamp(), expectedLast),
        result.getTimestamp().isAfter(expectedLast)
    );

    final TimeseriesResultValue value = result.getValue();

    Assert.assertEquals(result.toString(), 1870.061029, value.getDoubleMetric("maxIndex"), 1870.061029 * 1e-6);
    Assert.assertEquals(result.toString(), 59.021022, value.getDoubleMetric("minIndex"), 59.021022 * 1e-6);
  }


  @Override
  public void testEmptyTimeseries()
  {
    // Skip this test because the timeseries test expects the empty range to have one entry, but group by
    // does not expect anything
  }

  @Override
  public void testFullOnTimeseries()
  {
    // Skip this test because the timeseries test expects a skipped day to be filled in, but group by doesn't
    // fill anything in.
  }

  @Override
  public void testFullOnTimeseriesWithFilter()
  {
    // Skip this test because the timeseries test expects a skipped day to be filled in, but group by doesn't
    // fill anything in.
  }

  @Override
  public void testTimeseriesQueryZeroFilling()
  {
    // Skip this test because the timeseries test expects skipped hours to be filled in, but group by doesn't
    // fill anything in.
  }

  @Override
  public void testTimeseriesWithNonExistentFilter()
  {
    // Skip this test because the timeseries test expects a day that doesn't have a filter match to be filled in,
    // but group by just doesn't return a value if the filter doesn't match.
  }

  @Override
  public void testTimeseriesWithNonExistentFilterAndMultiDim()
  {
    // Skip this test because the timeseries test expects a day that doesn't have a filter match to be filled in,
    // but group by just doesn't return a value if the filter doesn't match.
  }

  @Override
  public void testTimeseriesWithFilterOnNonExistentDimension()
  {
    // Skip this test because the timeseries test expects a day that doesn't have a filter match to be filled in,
    // but group by just doesn't return a value if the filter doesn't match.
  }
}
