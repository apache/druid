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
