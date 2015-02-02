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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TimeseriesQueryRunnerBonusTest
{
  @Test
  public void testOneRowAtATime() throws Exception
  {
    final IncrementalIndex oneRowIndex = new OnheapIncrementalIndex(
        new DateTime("2012-01-01T00:00:00Z").getMillis(), QueryGranularity.NONE, new AggregatorFactory[]{}, 1000
    );

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

  private static List<Result<TimeseriesResultValue>> runTimeseriesCount(IncrementalIndex index)
  {
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(new QueryConfig()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    final QueryRunner<Result<TimeseriesResultValue>> runner = makeQueryRunner(
        factory,
        new IncrementalIndexSegment(index, null)
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("xxx")
                                  .granularity(QueryGranularity.ALL)
                                  .intervals(ImmutableList.of(new Interval("2012-01-01T00:00:00Z/P1D")))
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new CountAggregatorFactory("rows")
                                      )
                                  )
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
