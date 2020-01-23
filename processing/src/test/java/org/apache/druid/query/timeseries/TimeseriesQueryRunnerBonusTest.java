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

package org.apache.druid.query.timeseries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class TimeseriesQueryRunnerBonusTest
{
  @Parameterized.Parameters(name = "descending={0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(false, true));
  }

  private final boolean descending;

  public TimeseriesQueryRunnerBonusTest(boolean descending)
  {
    this.descending = descending;
  }

  @Test
  public void testOneRowAtATime() throws Exception
  {
    final IncrementalIndex oneRowIndex = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(DateTimes.of("2012-01-01T00:00:00Z").getMillis())
                .build()
        )
        .setMaxRowCount(1000)
        .buildOnheap();

    List<Result<TimeseriesResultValue>> results;

    oneRowIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2012-01-01T00:00:00Z").getMillis(),
            ImmutableList.of("dim1"),
            ImmutableMap.of("dim1", "x")
        )
    );

    results = runTimeseriesCount(oneRowIndex);

    Assert.assertEquals("index size", 1, oneRowIndex.size());
    Assert.assertEquals("result size", 1, results.size());
    Assert.assertEquals("result timestamp", DateTimes.of("2012-01-01T00:00:00Z"), results.get(0).getTimestamp());
    Assert.assertEquals("result count metric", 1, (long) results.get(0).getValue().getLongMetric("rows"));

    oneRowIndex.add(
        new MapBasedInputRow(
            DateTimes.of("2012-01-01T00:00:00Z").getMillis(),
            ImmutableList.of("dim1"),
            ImmutableMap.of("dim1", "y")
        )
    );

    results = runTimeseriesCount(oneRowIndex);

    Assert.assertEquals("index size", 2, oneRowIndex.size());
    Assert.assertEquals("result size", 1, results.size());
    Assert.assertEquals("result timestamp", DateTimes.of("2012-01-01T00:00:00Z"), results.get(0).getTimestamp());
    Assert.assertEquals("result count metric", 2, (long) results.get(0).getValue().getLongMetric("rows"));
  }

  private List<Result<TimeseriesResultValue>> runTimeseriesCount(IncrementalIndex index)
  {
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    final QueryRunner<Result<TimeseriesResultValue>> runner = makeQueryRunner(
        factory,
        new IncrementalIndexSegment(index, SegmentId.dummy("ds"))
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("xxx")
                                  .granularity(Granularities.ALL)
                                  .intervals(ImmutableList.of(Intervals.of("2012-01-01T00:00:00Z/P1D")))
                                  .aggregators(new CountAggregatorFactory("rows"))
                                  .descending(descending)
                                  .build();
    return runner.run(QueryPlus.wrap(query)).toList();
  }

  private static <T> QueryRunner<T> makeQueryRunner(QueryRunnerFactory<T, Query<T>> factory, Segment adapter)
  {
    return new FinalizeResultsQueryRunner<>(factory.createRunner(adapter), factory.getToolchest());
  }
}
