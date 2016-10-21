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

package io.druid.query.aggregation.variance;

import com.google.common.collect.Lists;

import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryRunnerTest;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@RunWith(Parameterized.class)
public class VarianceTimeseriesQueryTest
{
  @Parameterized.Parameters(name="{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return TimeseriesQueryRunnerTest.constructorFeeder();
  }

  private final QueryRunner runner;
  private final boolean descending;

  public VarianceTimeseriesQueryTest(QueryRunner runner, boolean descending)
  {
    this.runner = runner;
    this.descending = descending;
  }

  @Test
  public void testTimeseriesWithNullFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(VarianceTestHelper.dataSource)
                                  .granularity(VarianceTestHelper.dayGran)
                                  .filters("bobby", null)
                                  .intervals(VarianceTestHelper.firstToThird)
                                  .aggregators(VarianceTestHelper.commonPlusVarAggregators)
                                  .postAggregators(
                                      Arrays.<PostAggregator>asList(
                                          VarianceTestHelper.addRowsIndexConstant,
                                          VarianceTestHelper.stddevOfIndexPostAggr
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                VarianceTestHelper.of(
                    "rows", 13L,
                    "index", 6626.151596069336,
                    "addRowsIndexConstant", 6640.151596069336,
                    "uniques", VarianceTestHelper.UNIQUES_9,
                    "index_var", descending ? 368885.6897238851 : 368885.689155086,
                    "index_stddev", descending ? 607.3596049490657 : 607.35960448081
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                VarianceTestHelper.of(
                    "rows", 13L,
                    "index", 5833.2095947265625,
                    "addRowsIndexConstant", 5847.2095947265625,
                    "uniques", VarianceTestHelper.UNIQUES_9,
                    "index_var", descending ? 259061.6037088883 : 259061.60216419376,
                    "index_stddev", descending ? 508.9809463122252 : 508.98094479478675
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query, new HashMap<String, Object>()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
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
