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

package io.druid.query.aggregation.histogram;

import io.druid.query.aggregation.TestObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ApproximateHistogramPostAggregatorTest
{
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
    float[] values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    ApproximateHistogram ah = buildHistogram(10, values);
    final TestObjectColumnSelector selector = new TestObjectColumnSelector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    ApproximateHistogramAggregator agg = new ApproximateHistogramAggregator("price", selector, false, 10, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY);
    for (int i = 0; i < values.length; i++) {
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
