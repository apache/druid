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

package io.druid.query.aggregation;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class MinAggregatorTest
{
  private void aggregate(TestFloatColumnSelector selector, MinAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate() throws Exception
  {
    final float[] values = {0.15f, 0.27f, 0.0f, 0.93f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);
    MinAggregator agg = new MinAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);

    Assert.assertEquals(new Float(values[2]).doubleValue(), agg.get());
  }
}
