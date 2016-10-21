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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryRunnerTest;
import io.druid.query.timeseries.TimeseriesResultValue;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class GroupByTimeseriesQueryRunnerTest extends TimeseriesQueryRunnerTest
{
  @SuppressWarnings("unchecked")
  @Parameterized.Parameters(name="{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final GroupByQueryRunnerFactory factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(config);
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        Lists.transform(
            QueryRunnerTestHelper.makeQueryRunners(factory),
            new Function<QueryRunner<Row>, Object>()
            {
              @Nullable
              @Override
              public Object apply(final QueryRunner<Row> input)
              {
                return new QueryRunner()
                {
                  @Override
                  public Sequence run(Query query, Map responseContext)
                  {
                    TimeseriesQuery tsQuery = (TimeseriesQuery) query;

                    return Sequences.map(
                        input.run(
                            GroupByQuery.builder()
                                        .setDataSource(tsQuery.getDataSource())
                                        .setQuerySegmentSpec(tsQuery.getQuerySegmentSpec())
                                        .setGranularity(tsQuery.getGranularity())
                                        .setDimFilter(tsQuery.getDimensionsFilter())
                                        .setAggregatorSpecs(tsQuery.getAggregatorSpecs())
                                        .setPostAggregatorSpecs(tsQuery.getPostAggregatorSpecs())
                                        .build(),
                            responseContext
                        ),
                        new Function<Row, Result<TimeseriesResultValue>>()
                        {
                          @Override
                          public Result<TimeseriesResultValue> apply(final Row input)
                          {
                            MapBasedRow row = (MapBasedRow) input;

                            return new Result<TimeseriesResultValue>(
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
    super(runner, false);
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
