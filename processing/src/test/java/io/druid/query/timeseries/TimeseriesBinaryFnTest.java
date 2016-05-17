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

import com.google.common.collect.ImmutableMap;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class TimeseriesBinaryFnTest
{
  final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
  final LongSumAggregatorFactory indexLongSum = new LongSumAggregatorFactory("index", "index");
  final List<AggregatorFactory> aggregatorFactories = Arrays.asList(
      rowsCount,
      indexLongSum
  );
  final DateTime currTime = new DateTime();

  @Test
  public void testMerge()
  {
    Result<TimeseriesResultValue> result1 = new Result<TimeseriesResultValue>(
        currTime,
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 1L,
                "index", 2L
            )
        )
    );
    Result<TimeseriesResultValue> result2 = new Result<TimeseriesResultValue>(
        currTime,
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 2L,
                "index", 3L
            )
        )
    );

    Result<TimeseriesResultValue> expected = new Result<TimeseriesResultValue>(
        currTime,
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 3L,
                "index", 5L
            )
        )
    );

    Result<TimeseriesResultValue> actual = new TimeseriesBinaryFn(
        QueryGranularities.ALL,
        aggregatorFactories
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMergeDay()
  {
    Result<TimeseriesResultValue> result1 = new Result<TimeseriesResultValue>(
        currTime,
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 1L,
                "index", 2L
            )
        )
    );
    Result<TimeseriesResultValue> result2 = new Result<TimeseriesResultValue>(
        currTime,
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 2L,
                "index", 3L
            )
        )
    );

    Result<TimeseriesResultValue> expected = new Result<TimeseriesResultValue>(
        new DateTime(QueryGranularities.DAY.truncate(currTime.getMillis())),
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 3L,
                "index", 5L
            )
        )
    );

    Result<TimeseriesResultValue> actual = new TimeseriesBinaryFn(
        QueryGranularities.DAY,
        aggregatorFactories
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMergeOneNullResult()
  {
    Result<TimeseriesResultValue> result1 = new Result<TimeseriesResultValue>(
        currTime,
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 1L,
                "index", 2L
            )
        )
    );
    Result<TimeseriesResultValue> result2 = null;

    Result<TimeseriesResultValue> expected = result1;

    Result<TimeseriesResultValue> actual = new TimeseriesBinaryFn(
        QueryGranularities.ALL,
        aggregatorFactories
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMergeShiftedTimestamp()
  {
    Result<TimeseriesResultValue> result1 = new Result<TimeseriesResultValue>(
        currTime,
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 1L,
                "index", 2L
            )
        )
    );
    Result<TimeseriesResultValue> result2 = new Result<TimeseriesResultValue>(
        currTime.plusHours(2),
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 2L,
                "index", 3L
            )
        )
    );

    Result<TimeseriesResultValue> expected = new Result<TimeseriesResultValue>(
        currTime,
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 3L,
                "index", 5L
            )
        )
    );

    Result<TimeseriesResultValue> actual = new TimeseriesBinaryFn(
        QueryGranularities.ALL,
        aggregatorFactories
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected, actual);
  }
}
