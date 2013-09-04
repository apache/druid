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
public class LongSumAggregatorTest
{
  private void aggregate(TestFloatColumnSelector selector, LongSumAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(new float[]{24.15f, 20f});
    LongSumAggregator agg = new LongSumAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    Assert.assertEquals(0l, agg.get());
    Assert.assertEquals(0l, agg.get());
    Assert.assertEquals(0l, agg.get());
    aggregate(selector, agg);
    Assert.assertEquals(24l, agg.get());
    Assert.assertEquals(24l, agg.get());
    Assert.assertEquals(24l, agg.get());
    aggregate(selector, agg);
    Assert.assertEquals(44l, agg.get());
    Assert.assertEquals(44l, agg.get());
    Assert.assertEquals(44l, agg.get());
  }

  @Test
  public void testComparator()
  {
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(new float[]{18293f});
    LongSumAggregator agg = new LongSumAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    Object first = agg.get();
    agg.aggregate();

    Comparator comp = new LongSumAggregatorFactory("null", "null").getComparator();

    Assert.assertEquals(-1, comp.compare(first, agg.get()));
    Assert.assertEquals(0, comp.compare(first, first));
    Assert.assertEquals(0, comp.compare(agg.get(), agg.get()));
    Assert.assertEquals(1, comp.compare(agg.get(), first));
  }
}
