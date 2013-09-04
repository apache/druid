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

package io.druid.query.aggregation;

import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class DoubleSumAggregatorTest
{
  private void aggregate(TestFloatColumnSelector selector, DoubleSumAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    final float[] values = {0.15f, 0.27f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);
    DoubleSumAggregator agg = new DoubleSumAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;

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
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(new float[]{0.15f, 0.27f});
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
