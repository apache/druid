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

package com.metamx.druid.aggregation.post;

import java.util.Comparator;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ConstantPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    ConstantPostAggregator constantPostAggregator;

    constantPostAggregator = new ConstantPostAggregator("shichi", 7);
    Assert.assertEquals(7, constantPostAggregator.compute(null));
    constantPostAggregator = new ConstantPostAggregator("rei", 0.0);
    Assert.assertEquals(0.0, constantPostAggregator.compute(null));
    constantPostAggregator = new ConstantPostAggregator("ichi", 1.0);
    Assert.assertNotSame(1, constantPostAggregator.compute(null));
  }

  @Test
  public void testComparator()
  {
    ConstantPostAggregator constantPostAggregator =
        new ConstantPostAggregator("thistestbasicallydoesnothing unhappyface", 1);
    Comparator comp = constantPostAggregator.getComparator();
    Assert.assertEquals(0, comp.compare(0, constantPostAggregator.compute(null)));
    Assert.assertEquals(0, comp.compare(0, 1));
    Assert.assertEquals(0, comp.compare(1, 0));
  }
}
