/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.CountAggregatorFactory;
import com.metamx.druid.aggregation.LongSumAggregatorFactory;
import com.metamx.druid.aggregation.post.ArithmeticPostAggregator;
import com.metamx.druid.aggregation.post.ConstantPostAggregator;
import com.metamx.druid.aggregation.post.FieldAccessPostAggregator;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeseriesResultValue;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class TimeseriesBinaryFnTest
{
  final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
  final LongSumAggregatorFactory indexLongSum = new LongSumAggregatorFactory("index", "index");
  final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L);
  final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
  final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");
  final ArithmeticPostAggregator addRowsIndexConstant = new ArithmeticPostAggregator(
      "addRowsIndexConstant",
      "+",
      Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
  );
  final List<AggregatorFactory> aggregatorFactories = Arrays.asList(
      rowsCount,
      indexLongSum
  );
  final List<PostAggregator> postAggregators = Arrays.<PostAggregator>asList(
      addRowsIndexConstant
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
                "index", 5L,
                "addRowsIndexConstant", 9.0
            )
        )
    );

    Result<TimeseriesResultValue> actual = new TimeseriesBinaryFn(
        QueryGranularity.ALL,
        aggregatorFactories,
        postAggregators
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
        new DateTime(QueryGranularity.DAY.truncate(currTime.getMillis())),
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>of(
                "rows", 3L,
                "index", 5L,
                "addRowsIndexConstant", 9.0
            )
        )
    );

    Result<TimeseriesResultValue> actual = new TimeseriesBinaryFn(
        QueryGranularity.DAY,
        aggregatorFactories,
        postAggregators
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
        QueryGranularity.ALL,
        aggregatorFactories,
        postAggregators
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
                "index", 5L,
                "addRowsIndexConstant", 9.0
            )
        )
    );

    Result<TimeseriesResultValue> actual = new TimeseriesBinaryFn(
        QueryGranularity.ALL,
        aggregatorFactories,
        postAggregators
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected, actual);
  }
}
