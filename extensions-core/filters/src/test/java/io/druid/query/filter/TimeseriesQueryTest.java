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

package io.druid.query.filter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TimeseriesQueryTest
{
  public static final Map<String, Object> CONTEXT = ImmutableMap.of();

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.cartesian(
        // runners
        QueryRunnerTestHelper.makeQueryRunners(
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(
                    QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                ),
                new TimeseriesQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        ),
        // descending?
        Arrays.asList(false, true)
    );
  }

  private final QueryRunner runner;
  private final boolean descending;

  public TimeseriesQueryTest(
      QueryRunner runner, boolean descending
  )
  {
    this.runner = runner;
    this.descending = descending;
  }

  @Test
  public void testTimeSeriesWithFilteredAggWithSelectDimFilter()
  {
    Druids.TimeseriesQueryBuilder builder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.firstToThird)
              .descending(descending);

    // existing
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, ">", "entertainment", 20);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, ">=", "entertainment", 22);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<", "entertainment", 4);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<=", "entertainment", 6);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "==", "entertainment", 2);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<>", "entertainment", 24);

    // non-existing (between)
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, ">", "financial", 20);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, ">=", "financial", 20);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<", "financial", 6);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<=", "financial", 6);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "==", "financial", 0);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<>", "financial", 26);

    // non-existing (smaller than min)
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, ">", "abcb", 26);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, ">=", "abcb", 26);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<", "abcb", 0);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<=", "abcb", 0);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "==", "abcb", 0);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<>", "abcb", 26);

    // non-existing (bigger than max)
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, ">", "zztop", 0);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, ">=", "zztop", 0);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<", "zztop", 26);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<=", "zztop", 26);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "==", "zztop", 0);
    validateSelectDimFilter(builder, QueryRunnerTestHelper.qualityDimension, "<>", "zztop", 26);

    // null
    validateSelectDimFilter(builder, "partial_null_column", "==", "", 22);
    validateSelectDimFilter(builder, "partial_null_column", "<>", "", 4);
  }

  private void validateSelectDimFilter(
      Druids.TimeseriesQueryBuilder builder,
      String dimension,
      String operation,
      String value,
      long expected
  )
  {
    builder.aggregators(
        Arrays.<AggregatorFactory>asList(
            new FilteredAggregatorFactory(
                new CountAggregatorFactory("filteredAgg"),
                new SelectorDimFilterExtension(dimension, value, operation)
            )
        )
    );
    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(builder.build(), ImmutableMap.of()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Iterator<Result<TimeseriesResultValue>> iterator = results.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(expected, iterator.next().getValue().getLongMetric("filteredAgg").longValue());
    Assert.assertFalse(iterator.hasNext());
  }
}
