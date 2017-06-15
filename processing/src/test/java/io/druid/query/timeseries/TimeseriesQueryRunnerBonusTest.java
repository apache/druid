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

package io.druid.query.timeseries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@RunWith(Parameterized.class)
public class TimeseriesQueryRunnerBonusTest
{
  @Parameterized.Parameters(name = "descending={0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
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
                .withMinTimestamp(new DateTime("2012-01-01T00:00:00Z").getMillis())
                .build()
        )
        .setMaxRowCount(1000)
        .buildOnheap();

    List<Result<TimeseriesResultValue>> results;

    oneRowIndex.add(
        new MapBasedInputRow(
            new DateTime("2012-01-01T00:00:00Z").getMillis(),
            ImmutableList.of("dim1"),
            ImmutableMap.<String, Object>of("dim1", "x")
        )
    );

    results = runTimeseriesCount(oneRowIndex);

    Assert.assertEquals("index size", 1, oneRowIndex.size());
    Assert.assertEquals("result size", 1, results.size());
    Assert.assertEquals("result timestamp", new DateTime("2012-01-01T00:00:00Z"), results.get(0).getTimestamp());
    Assert.assertEquals("result count metric", 1, (long) results.get(0).getValue().getLongMetric("rows"));

    oneRowIndex.add(
        new MapBasedInputRow(
            new DateTime("2012-01-01T00:00:00Z").getMillis(),
            ImmutableList.of("dim1"),
            ImmutableMap.<String, Object>of("dim1", "y")
        )
    );

    results = runTimeseriesCount(oneRowIndex);

    Assert.assertEquals("index size", 2, oneRowIndex.size());
    Assert.assertEquals("result size", 1, results.size());
    Assert.assertEquals("result timestamp", new DateTime("2012-01-01T00:00:00Z"), results.get(0).getTimestamp());
    Assert.assertEquals("result count metric", 2, (long) results.get(0).getValue().getLongMetric("rows"));
  }

  private List<Result<TimeseriesResultValue>> runTimeseriesCount(IncrementalIndex index)
  {
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    final QueryRunner<Result<TimeseriesResultValue>> runner = makeQueryRunner(
        factory,
        new IncrementalIndexSegment(index, null)
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("xxx")
                                  .granularity(Granularities.ALL)
                                  .intervals(ImmutableList.of(new Interval("2012-01-01T00:00:00Z/P1D")))
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new CountAggregatorFactory("rows")
                                      )
                                  )
                                  .descending(descending)
                                  .build();
    HashMap<String,Object> context = new HashMap<String, Object>();
    return Sequences.toList(
        runner.run(query, context),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
  }

  private static <T> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      Segment adapter
  )
  {
    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }
}
