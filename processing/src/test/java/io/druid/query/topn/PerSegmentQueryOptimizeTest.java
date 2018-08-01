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

package io.druid.query.topn;

import io.druid.java.util.common.Intervals;
import io.druid.query.PerSegmentQueryOptimizationContext;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.SuppressedAggregatorFactory;
import io.druid.query.filter.IntervalDimFilter;
import io.druid.segment.column.Column;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class PerSegmentQueryOptimizeTest
{
  @Test
  public void testFilteredAggregatorOptimize()
  {
    LongSumAggregatorFactory longSumAggregatorFactory = new LongSumAggregatorFactory("test", "test");

    FilteredAggregatorFactory aggregatorFactory = new FilteredAggregatorFactory(
        longSumAggregatorFactory,
        new IntervalDimFilter(
            Column.TIME_COLUMN_NAME,
            Collections.singletonList(Intervals.utc(1000, 2000)),
            null
        )
    );

    Interval exclude = Intervals.utc(2000, 3000);
    Interval include = Intervals.utc(1500, 1600);
    Interval partial = Intervals.utc(1500, 2500);

    AggregatorFactory excludedAgg = aggregatorFactory.optimizeForSegment(getOptimizationContext(exclude));
    AggregatorFactory expectedSuppressedAgg = new SuppressedAggregatorFactory(longSumAggregatorFactory);
    Assert.assertEquals(expectedSuppressedAgg, excludedAgg);

    AggregatorFactory includedAgg = aggregatorFactory.optimizeForSegment(getOptimizationContext(include));
    Assert.assertEquals(longSumAggregatorFactory, includedAgg);

    AggregatorFactory partialAgg = aggregatorFactory.optimizeForSegment(getOptimizationContext(partial));
    AggregatorFactory expectedPartialFilteredAgg = new FilteredAggregatorFactory(
        longSumAggregatorFactory,
        new IntervalDimFilter(
            Column.TIME_COLUMN_NAME,
            Collections.singletonList(Intervals.utc(1500, 2000)),
            null
        )
    );
    Assert.assertEquals(expectedPartialFilteredAgg, partialAgg);
  }

  @Test
  public void testFilteredAggregatorDontOptimizeOnNonTimeColumn()
  {
    // Filter is not on __time, so no optimizations should be made.
    LongSumAggregatorFactory longSumAggregatorFactory = new LongSumAggregatorFactory("test", "test");

    FilteredAggregatorFactory aggregatorFactory = new FilteredAggregatorFactory(
        longSumAggregatorFactory,
        new IntervalDimFilter(
            "not_time",
            Collections.singletonList(Intervals.utc(1000, 2000)),
            null
        )
    );

    Interval exclude = Intervals.utc(2000, 3000);
    Interval include = Intervals.utc(1500, 1600);
    Interval partial = Intervals.utc(1500, 2500);

    AggregatorFactory excludedAgg = aggregatorFactory.optimizeForSegment(getOptimizationContext(exclude));
    Assert.assertEquals(aggregatorFactory, excludedAgg);

    AggregatorFactory includedAgg = aggregatorFactory.optimizeForSegment(getOptimizationContext(include));
    Assert.assertEquals(aggregatorFactory, includedAgg);

    AggregatorFactory partialAgg = aggregatorFactory.optimizeForSegment(getOptimizationContext(partial));
    Assert.assertEquals(aggregatorFactory, partialAgg);
  }

  private PerSegmentQueryOptimizationContext getOptimizationContext(Interval segmentInterval)
  {
    return new PerSegmentQueryOptimizationContext(
        new SegmentDescriptor(segmentInterval, "0", 0)
    );
  }
}
