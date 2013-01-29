package com.metamx.druid.query.timeseries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Druids;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.CountAggregatorFactory;
import com.metamx.druid.index.IncrementalIndexSegment;
import com.metamx.druid.index.Segment;
import com.metamx.druid.index.v1.IncrementalIndex;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.query.FinalizeResultsQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeseriesResultValue;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.List;

public class TimeseriesQueryRunnerBonusTest
{
  @Test
  public void testOneRowAtATime() throws Exception
  {
    final IncrementalIndex oneRowIndex = new IncrementalIndex(
        new DateTime("2012-01-01T00:00:00Z").getMillis(), QueryGranularity.NONE, new AggregatorFactory[]{}
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
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory();
    final QueryRunner<Result<TimeseriesResultValue>> runner = makeQueryRunner(
        factory,
        new IncrementalIndexSegment(index)
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

    return Sequences.toList(
        runner.run(query),
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
