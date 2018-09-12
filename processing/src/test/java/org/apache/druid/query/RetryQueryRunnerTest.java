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

package org.apache.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RetryQueryRunnerTest
{
  private static class TestRetryQueryRunnerConfig extends RetryQueryRunnerConfig
  {
    private int numTries;
    private boolean returnPartialResults;

    public TestRetryQueryRunnerConfig(int numTries, boolean returnPartialResults)
    {
      this.numTries = numTries;
      this.returnPartialResults = returnPartialResults;
    }

    @Override
    public int getNumTries()
    {
      return numTries;
    }

    @Override
    public boolean isReturnPartialResults()
    {
      return returnPartialResults;
    }
  }

  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

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
  public void testRunWithMissingSegments()
  {
    Map<String, Object> context = new ConcurrentHashMap<>();
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(QueryPlus queryPlus, Map context)
          {
            ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
                new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1)
            );
            return Sequences.empty();
          }
        },
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

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();

    Assert.assertTrue(
        "Should have one entry in the list of missing segments",
        ((List) context.get(Result.MISSING_SEGMENTS_KEY)).size() == 1
    );
    Assert.assertTrue("Should return an empty sequence as a result", ((List) actualResults).size() == 0);
  }


  @Test
  public void testRetry()
  {
    Map<String, Object> context = new ConcurrentHashMap<>();
    context.put("count", 0);
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              Map<String, Object> context
          )
          {
            if ((int) context.get("count") == 0) {
              ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
                  new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1)
              );
              context.put("count", 1);
              return Sequences.empty();
            } else {
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              Maps.newHashMap()
                          )
                      )
                  )
              );
            }
          }
        },
        new TestRetryQueryRunnerConfig(1, true),
        jsonMapper
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();

    Assert.assertTrue("Should return a list with one element", ((List) actualResults).size() == 1);
    Assert.assertTrue(
        "Should have nothing in missingSegment list",
        ((List) context.get(Result.MISSING_SEGMENTS_KEY)).size() == 0
    );
  }

  @Test
  public void testRetryMultiple()
  {
    Map<String, Object> context = new ConcurrentHashMap<>();
    context.put("count", 0);
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              Map<String, Object> context
          )
          {
            if ((int) context.get("count") < 3) {
              ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
                  new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1)
              );
              context.put("count", (int) context.get("count") + 1);
              return Sequences.empty();
            } else {
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              Maps.newHashMap()
                          )
                      )
                  )
              );
            }
          }
        },
        new TestRetryQueryRunnerConfig(4, true),
        jsonMapper
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();

    Assert.assertTrue("Should return a list with one element", ((List) actualResults).size() == 1);
    Assert.assertTrue(
        "Should have nothing in missingSegment list",
        ((List) context.get(Result.MISSING_SEGMENTS_KEY)).size() == 0
    );
  }

  @Test(expected = SegmentMissingException.class)
  public void testException()
  {
    Map<String, Object> context = new ConcurrentHashMap<>();
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              Map<String, Object> context
          )
          {
            ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
                new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1)
            );
            return Sequences.empty();
          }
        },
        new TestRetryQueryRunnerConfig(1, false),
        jsonMapper
    );

    runner.run(QueryPlus.wrap(query), context).toList();

    Assert.assertTrue(
        "Should have one entry in the list of missing segments",
        ((List) context.get(Result.MISSING_SEGMENTS_KEY)).size() == 1
    );
  }

  @Test
  public void testNoDuplicateRetry()
  {
    Map<String, Object> context = new ConcurrentHashMap<>();
    context.put("count", 0);
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              Map<String, Object> context
          )
          {
            final Query<Result<TimeseriesResultValue>> query = queryPlus.getQuery();
            if ((int) context.get("count") == 0) {
              // assume 2 missing segments at first run
              ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
                  new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1)
              );
              ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
                  new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 2)
              );
              context.put("count", 1);
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              Maps.newHashMap()
                          )
                      )
                  )
              );
            } else if ((int) context.get("count") == 1) {
              // this is first retry
              Assert.assertTrue("Should retry with 2 missing segments", ((MultipleSpecificSegmentSpec) ((BaseQuery) query).getQuerySegmentSpec()).getDescriptors().size() == 2);
              // assume only left 1 missing at first retry
              ((List) context.get(Result.MISSING_SEGMENTS_KEY)).add(
                  new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 2)
              );
              context.put("count", 2);
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              Maps.newHashMap()
                          )
                      )
                  )
              );
            } else {
              // this is second retry
              Assert.assertTrue("Should retry with 1 missing segments", ((MultipleSpecificSegmentSpec) ((BaseQuery) query).getQuerySegmentSpec()).getDescriptors().size() == 1);
              // assume no more missing at second retry
              context.put("count", 3);
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              Maps.newHashMap()
                          )
                      )
                  )
              );
            }
          }
        },
        new TestRetryQueryRunnerConfig(2, false),
        jsonMapper
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();

    Assert.assertTrue("Should return a list with 3 elements", ((List) actualResults).size() == 3);
    Assert.assertTrue(
        "Should have nothing in missingSegment list",
        ((List) context.get(Result.MISSING_SEGMENTS_KEY)).size() == 0
    );
  }
}
