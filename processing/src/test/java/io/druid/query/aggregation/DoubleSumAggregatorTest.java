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

package io.druid.query.aggregation;

import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class DoubleSumAggregatorTest
{
  private void aggregate(TestDoubleColumnSelector selector, DoubleSumAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    final double[] values = {0.15d, 0.27d};
    final TestDoubleColumnSelector selector = new TestDoubleColumnSelector(values);
    DoubleSumAggregator agg = new DoubleSumAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    double expectedFirst = values[0];
    double expectedSecond = values[1] + expectedFirst;

    Assert.assertEquals(0.0d, agg.get());
    Assert.assertEquals(0.0d, agg.get());
    Assert.assertEquals(0.0d, agg.get());
    aggregate(selector, agg);
    Assert.assertEquals(expectedFirst, agg.get());
    Assert.assertEquals(expectedFirst, agg.get());
    Assert.assertEquals(expectedFirst, agg.get());
    aggregate(selector, agg);
    Assert.assertEquals(expectedSecond, agg.get());
    Assert.assertEquals(expectedSecond, agg.get());
    Assert.assertEquals(expectedSecond, agg.get());
  }

  @Test
  public void testComparator()
  {
    final TestDoubleColumnSelector selector = new TestDoubleColumnSelector(new double[]{0.15d, 0.27d});
    DoubleSumAggregator agg = new DoubleSumAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    Object first = agg.get();
    agg.aggregate();

    Comparator comp = new DoubleSumAggregatorFactory("null", "null").getComparator();

    Assert.assertEquals(-1, comp.compare(first, agg.get()));
    Assert.assertEquals(0, comp.compare(first, first));
    Assert.assertEquals(0, comp.compare(agg.get(), agg.get()));
    Assert.assertEquals(1, comp.compare(agg.get(), first));
  }
}
