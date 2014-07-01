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

package io.druid.query.aggregation.histogram;

import io.druid.query.aggregation.TestFloatColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ApproximateHistogramPostAggregatorTest
{
  static final float[] VALUES = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  protected ApproximateHistogram buildHistogram(int size, float[] values)
  {
    ApproximateHistogram h = new ApproximateHistogram(size);
    for (float v : values) {
      h.offer(v);
    }
    return h;
  }

  @Test
  public void testCompute()
  {
    ApproximateHistogram ah = buildHistogram(10, VALUES);
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(VALUES);

    ApproximateHistogramAggregator agg = new ApproximateHistogramAggregator("price", selector, 10, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY);
    for (int i = 0; i < VALUES.length; i++) {
      agg.aggregate();
      selector.increment();
    }

    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put(agg.getName(), agg.get());

    ApproximateHistogramPostAggregator approximateHistogramPostAggregator = new EqualBucketsPostAggregator(
        "approxHist",
        "price",
        5
    );
    Assert.assertEquals(ah.toHistogram(5), approximateHistogramPostAggregator.compute(metricValues));
  }

}
