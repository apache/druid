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

package org.apache.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Aggregate test for {@link LongSumAggregatorFactory}.
 */
@RunWith(Parameterized.class)
public class LongSumAggregateTest extends AggregateTestBase
{
  static final Interval INTERVAL = Intervals.of("2020-01-01/P1D");
  static final Granularity SEGMENT_GRANULARITY = Granularities.HOUR;
  static final int NUM_SEGMENT_PER_TIME_PARTITION = 2;
  static final int NUM_ROWS_PER_SEGMENT = 10;
  static final double NULL_RATIO = 0.2;

  final LongSumAggregatorFactory aggregatorFactory = new LongSumAggregatorFactory(
      TestColumn.LONG_COLUMN.getName(),
      TestColumn.LONG_COLUMN.getName()
  );

  @Parameters
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{false, false},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{true, true}
    );
  }

  public LongSumAggregateTest(boolean rollup, boolean persist)
  {
    super(
        INTERVAL,
        SEGMENT_GRANULARITY,
        NUM_SEGMENT_PER_TIME_PARTITION,
        NUM_ROWS_PER_SEGMENT,
        NULL_RATIO,
        null,
        rollup,
        persist
    );
  }

  @Test
  public void testAggregate()
  {
    Assert.assertEquals(
        compute(
            TestColumn.LONG_COLUMN,
            INTERVAL,
            valueExtractFunction(),
            accumulateFunction(),
            isReplaceNullWithDefault() ? 0L : null
        ),
        aggregate(aggregatorFactory, INTERVAL)
    );
  }

  @Test
  public void testBufferAggregate()
  {
    Assert.assertEquals(
        compute(
            TestColumn.LONG_COLUMN,
            INTERVAL,
            valueExtractFunction(),
            accumulateFunction(),
            isReplaceNullWithDefault() ? 0L : null
        ),
        bufferAggregate(aggregatorFactory, INTERVAL)
    );
  }

  private static Function<Long, Long> valueExtractFunction()
  {
    return val -> {
      if (val == null) {
        return isReplaceNullWithDefault() ? 0L : null;
      } else {
        return val;
      }
    };
  }

  private static BiFunction<Long, Long, Long> accumulateFunction()
  {
    return (v1, v2) -> {
      if (isReplaceNullWithDefault()) {
        // v1 and v2 cannot be null
        return v1 + v2;
      } else {
        if (v1 == null) {
          return v2;
        } else if (v2 == null) {
          return v1;
        } else {
          return v1 + v2;
        }
      }
    };
  }
}
