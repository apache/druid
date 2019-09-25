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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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
                                      .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                      .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                      .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                      .aggregators(
                                          Arrays.asList(
                                              QueryRunnerTestHelper.ROWS_COUNT,
                                              new LongSumAggregatorFactory(
                                                  "idx",
                                                  "index"
                                              ),
                                              QueryRunnerTestHelper.QUALITY_UNIQUES
                                          )
                                      )
                                      .build();


  @Test
  public void testRunWithMissingSegments()
  {
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.put(ResponseContext.Key.MISSING_SEGMENTS, new ArrayList<>());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(QueryPlus queryPlus, ResponseContext context)
          {
            context.add(
                ResponseContext.Key.MISSING_SEGMENTS,
                Collections.singletonList(new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1))
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
        ((List) context.get(ResponseContext.Key.MISSING_SEGMENTS)).size() == 1
    );
    Assert.assertTrue("Should return an empty sequence as a result", ((List) actualResults).size() == 0);
  }


  @Test
  public void testRetry()
  {
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.put(ResponseContext.Key.NUM_SCANNED_ROWS, 0);
    context.put(ResponseContext.Key.MISSING_SEGMENTS, new ArrayList<>());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              ResponseContext context
          )
          {
            if ((int) context.get(ResponseContext.Key.NUM_SCANNED_ROWS) == 0) {
              context.add(
                  ResponseContext.Key.MISSING_SEGMENTS,
                  Collections.singletonList(new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1))
              );
              context.put(ResponseContext.Key.NUM_SCANNED_ROWS, 1);
              return Sequences.empty();
            } else {
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              new HashMap<>()
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
        ((List) context.get(ResponseContext.Key.MISSING_SEGMENTS)).size() == 0
    );
  }

  @Test
  public void testRetryMultiple()
  {
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.put(ResponseContext.Key.NUM_SCANNED_ROWS, 0);
    context.put(ResponseContext.Key.MISSING_SEGMENTS, new ArrayList<>());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              ResponseContext context
          )
          {
            if ((int) context.get(ResponseContext.Key.NUM_SCANNED_ROWS) < 3) {
              context.add(
                  ResponseContext.Key.MISSING_SEGMENTS,
                  Collections.singletonList(new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1))
              );
              context.put(ResponseContext.Key.NUM_SCANNED_ROWS, (int) context.get(ResponseContext.Key.NUM_SCANNED_ROWS) + 1);
              return Sequences.empty();
            } else {
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              new HashMap<>()
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
        ((List) context.get(ResponseContext.Key.MISSING_SEGMENTS)).size() == 0
    );
  }

  @Test(expected = SegmentMissingException.class)
  public void testException()
  {
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.put(ResponseContext.Key.MISSING_SEGMENTS, new ArrayList<>());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              ResponseContext context
          )
          {
            context.add(
                ResponseContext.Key.MISSING_SEGMENTS,
                Collections.singletonList(new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1))
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
        ((List) context.get(ResponseContext.Key.MISSING_SEGMENTS)).size() == 1
    );
  }

  @Test
  public void testNoDuplicateRetry()
  {
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.put(ResponseContext.Key.NUM_SCANNED_ROWS, 0);
    context.put(ResponseContext.Key.MISSING_SEGMENTS, new ArrayList<>());
    RetryQueryRunner<Result<TimeseriesResultValue>> runner = new RetryQueryRunner<>(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              ResponseContext context
          )
          {
            final Query<Result<TimeseriesResultValue>> query = queryPlus.getQuery();
            if ((int) context.get(ResponseContext.Key.NUM_SCANNED_ROWS) == 0) {
              // assume 2 missing segments at first run
              context.add(
                  ResponseContext.Key.MISSING_SEGMENTS,
                  Arrays.asList(
                      new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 1),
                      new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 2)
                  )
              );
              context.put(ResponseContext.Key.NUM_SCANNED_ROWS, 1);
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              new HashMap<>()
                          )
                      )
                  )
              );
            } else if ((int) context.get(ResponseContext.Key.NUM_SCANNED_ROWS) == 1) {
              // this is first retry
              Assert.assertTrue("Should retry with 2 missing segments", ((MultipleSpecificSegmentSpec) ((BaseQuery) query).getQuerySegmentSpec()).getDescriptors().size() == 2);
              // assume only left 1 missing at first retry
              context.add(
                  ResponseContext.Key.MISSING_SEGMENTS,
                  Collections.singletonList(new SegmentDescriptor(Intervals.utc(178888, 1999999), "test", 2))
              );
              context.put(ResponseContext.Key.NUM_SCANNED_ROWS, 2);
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              new HashMap<>()
                          )
                      )
                  )
              );
            } else {
              // this is second retry
              Assert.assertTrue("Should retry with 1 missing segments", ((MultipleSpecificSegmentSpec) ((BaseQuery) query).getQuerySegmentSpec()).getDescriptors().size() == 1);
              // assume no more missing at second retry
              context.put(ResponseContext.Key.NUM_SCANNED_ROWS, 3);
              return Sequences.simple(
                  Collections.singletonList(
                      new Result<>(
                          DateTimes.nowUtc(),
                          new TimeseriesResultValue(
                              new HashMap<>()
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
        ((List) context.get(ResponseContext.Key.MISSING_SEGMENTS)).size() == 0
    );
  }
}
