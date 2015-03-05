/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
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
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(Query query, Map context)
          {
            ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
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
        (QueryToolChest) new TimeseriesQueryQueryToolChest(
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new RetryQueryRunnerConfig()
        {
          @Override
          public int getNumTries()
          {
            return 0;
          }

          @Override
          public boolean isReturnPartialResults()
          {
            return true;
          }
        },
        jsonMapper
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    Assert.assertTrue(
        "Should have one entry in the list of missing segments",
        ((List) context.get(Result.MISSING_SEGMENTS_KEY)).size() == 1
    );
    Assert.assertTrue("Should return an empty sequence as a result", ((List) actualResults).size() == 0);
  }


  @Test
  public void testRetry() throws Exception
  {
    Map<String, Object> context = new MapMaker().makeMap();
    context.put("count", 0);
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
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
              ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
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
        (QueryToolChest) new TimeseriesQueryQueryToolChest(
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new RetryQueryRunnerConfig()
        {
          private int numTries = 1;
          private boolean returnPartialResults = true;

          public int getNumTries() { return numTries; }

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
        ((List) context.get(Result.MISSING_SEGMENTS_KEY)).size() == 0
    );
  }

  @Test
  public void testRetryMultiple() throws Exception
  {
    Map<String, Object> context = new MapMaker().makeMap();
    context.put("count", 0);
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
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
              ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
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
        (QueryToolChest) new TimeseriesQueryQueryToolChest(
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new RetryQueryRunnerConfig()
        {
          private int numTries = 4;
          private boolean returnPartialResults = true;

          public int getNumTries() { return numTries; }

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
        ((List) context.get(Result.MISSING_SEGMENTS_KEY)).size() == 0
    );
  }

  @Test(expected = SegmentMissingException.class)
  public void testException() throws Exception
  {
    Map<String, Object> context = new MapMaker().makeMap();
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              Query<Result<TimeseriesResultValue>> query,
              Map<String, Object> context
          )
          {
            ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
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
        (QueryToolChest) new TimeseriesQueryQueryToolChest(
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new RetryQueryRunnerConfig()
        {
          private int numTries = 1;
          private boolean returnPartialResults = false;

          public int getNumTries() { return numTries; }

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
        ((List) context.get(Result.MISSING_SEGMENTS_KEY)).size() == 1
    );
  }
}
