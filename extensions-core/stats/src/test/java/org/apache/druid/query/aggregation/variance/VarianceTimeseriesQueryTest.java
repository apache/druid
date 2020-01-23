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

package org.apache.druid.query.aggregation.variance;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerTest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RunWith(Parameterized.class)
public class VarianceTimeseriesQueryTest extends InitializedNullHandlingTest
{
  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    // Use TimeseriesQueryRunnerTest's constructorFeeder, but remove vectorized tests, since this aggregator
    // can't vectorize yet.
    return StreamSupport.stream(TimeseriesQueryRunnerTest.constructorFeeder().spliterator(), false)
                        .filter(constructor -> !((boolean) constructor[2]) /* !vectorize */)
                        .map(constructor -> new Object[]{constructor[0], constructor[1], constructor[3]})
                        .collect(Collectors.toList());
  }

  private final QueryRunner runner;
  private final boolean descending;

  public VarianceTimeseriesQueryTest(
      QueryRunner runner,
      boolean descending,
      List<AggregatorFactory> aggregatorFactories
  )
  {
    this.runner = runner;
    this.descending = descending;
  }

  @Test
  public void testTimeseriesWithNullFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters("bobby", null)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(VarianceTestHelper.COMMON_PLUS_VAR_AGGREGATORS)
                                  .postAggregators(
                                      QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT,
                                      VarianceTestHelper.STD_DEV_OF_INDEX_POST_AGGR
                                  )
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                QueryRunnerTestHelper.of(
                    "rows", 13L,
                    "index", 6626.151596069336,
                    "addRowsIndexConstant", 6640.151596069336,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9,
                    "index_var", descending ? 368885.6897238851 : 368885.689155086,
                    "index_stddev", descending ? 607.3596049490657 : 607.35960448081
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                QueryRunnerTestHelper.of(
                    "rows", 13L,
                    "index", 5833.2095947265625,
                    "addRowsIndexConstant", 5847.2095947265625,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9,
                    "index_var", descending ? 259061.6037088883 : 259061.60216419376,
                    "index_stddev", descending ? 508.9809463122252 : 508.98094479478675
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  private <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results)
  {
    if (descending) {
      expectedResults = TestHelper.revert(expectedResults);
    }
    TestHelper.assertExpectedResults(expectedResults, results);
  }
}
