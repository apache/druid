package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.SegmentMissingException;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RetryQueryRunnerTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                      .dataSource(QueryRunnerTestHelper.dataSource)
                                      .granularity(QueryRunnerTestHelper.dayGran)
                                      .intervals(QueryRunnerTestHelper.firstToThird)
                                      .aggregators(
                                          Arrays.asList(
                                              QueryRunnerTestHelper.rowsCount,
                                              new LongSumAggregatorFactory(
                                                  "idx",
                                                  "index"
                                              ),
                                              QueryRunnerTestHelper.qualityUniques
                                          )
                                      )
                                      .build();


  @Test
  public void testRunWithMissingSegments() throws Exception
  {
    Map<String, Object> context = new MapMaker().makeMap();
    context.put(RetryQueryRunner.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(Query query, Map context)
          {
            ((List) context.get(RetryQueryRunner.MISSING_SEGMENTS_KEY)).add(
                new SegmentDescriptor(
                    new Interval(
                        178888,
                        1999999
                    ), "test", 1
                )
            );
            return Sequences.empty();
          }
        },
        new RetryQueryRunnerConfig()
        {
          private int numTries = 0;
          private boolean returnPartialResults = true;

          public int numTries() { return numTries; }

          public boolean returnPartialResults() { return returnPartialResults; }
        },
        jsonMapper
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    Assert.assertTrue(
        "Should have one entry in the list of missing segments",
        ((List) context.get(RetryQueryRunner.MISSING_SEGMENTS_KEY)).size() == 1
    );
    Assert.assertTrue("Should return an empty sequence as a result", ((List) actualResults).size() == 0);
  }


  @Test
  public void testRetry() throws Exception
  {
    Map<String, Object> context = new MapMaker().makeMap();
    context.put("count", 0);
    context.put(RetryQueryRunner.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              Query<Result<TimeseriesResultValue>> query,
              Map<String, Object> context
          )
          {
            if ((int) context.get("count") == 0) {
              ((List) context.get(RetryQueryRunner.MISSING_SEGMENTS_KEY)).add(
                  new SegmentDescriptor(
                      new Interval(
                          178888,
                          1999999
                      ), "test", 1
                  )
              );
              context.put("count", 1);
              return Sequences.empty();
            } else {
              return Sequences.simple(
                  Arrays.asList(
                      new Result<>(
                          new DateTime(),
                          new TimeseriesResultValue(
                              Maps.<String, Object>newHashMap()
                          )
                      )
                  )
              );
            }
          }
        },
        new RetryQueryRunnerConfig()
        {
          private int numTries = 1;
          private boolean returnPartialResults = true;

          public int numTries() { return numTries; }

          public boolean returnPartialResults() { return returnPartialResults; }
        },
        jsonMapper
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    Assert.assertTrue("Should return a list with one element", ((List) actualResults).size() == 1);
    Assert.assertTrue(
        "Should have nothing in missingSegment list",
        ((List) context.get(RetryQueryRunner.MISSING_SEGMENTS_KEY)).size() == 0
    );
  }

  @Test
  public void testRetryMultiple() throws Exception
  {
    Map<String, Object> context = new MapMaker().makeMap();
    context.put("count", 0);
    context.put(RetryQueryRunner.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              Query<Result<TimeseriesResultValue>> query,
              Map<String, Object> context
          )
          {
            if ((int) context.get("count") < 3) {
              ((List) context.get(RetryQueryRunner.MISSING_SEGMENTS_KEY)).add(
                  new SegmentDescriptor(
                      new Interval(
                          178888,
                          1999999
                      ), "test", 1
                  )
              );
              context.put("count", (int) context.get("count") + 1);
              return Sequences.empty();
            } else {
              return Sequences.simple(
                  Arrays.asList(
                      new Result<>(
                          new DateTime(),
                          new TimeseriesResultValue(
                              Maps.<String, Object>newHashMap()
                          )
                      )
                  )
              );
            }
          }
        },
        new RetryQueryRunnerConfig()
        {
          private int numTries = 4;
          private boolean returnPartialResults = true;

          public int numTries() { return numTries; }

          public boolean returnPartialResults() { return returnPartialResults; }
        },
        jsonMapper
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    Assert.assertTrue("Should return a list with one element", ((List) actualResults).size() == 1);
    Assert.assertTrue(
        "Should have nothing in missingSegment list",
        ((List) context.get(RetryQueryRunner.MISSING_SEGMENTS_KEY)).size() == 0
    );
  }

  @Test(expected = SegmentMissingException.class)
  public void testException() throws Exception
  {
    Map<String, Object> context = new MapMaker().makeMap();
    context.put(RetryQueryRunner.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              Query<Result<TimeseriesResultValue>> query,
              Map<String, Object> context
          )
          {
            ((List) context.get(RetryQueryRunner.MISSING_SEGMENTS_KEY)).add(
                new SegmentDescriptor(
                    new Interval(
                        178888,
                        1999999
                    ), "test", 1
                )
            );
            return Sequences.empty();
          }
        },
        new RetryQueryRunnerConfig()
        {
          private int numTries = 1;
          private boolean returnPartialResults = false;

          public int numTries() { return numTries; }

          public boolean returnPartialResults() { return returnPartialResults; }
        },
        jsonMapper
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    Assert.assertTrue(
        "Should have one entry in the list of missing segments",
        ((List) context.get(RetryQueryRunner.MISSING_SEGMENTS_KEY)).size() == 1
    );
  }
}
