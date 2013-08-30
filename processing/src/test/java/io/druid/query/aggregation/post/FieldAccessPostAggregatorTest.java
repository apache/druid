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

package io.druid.query.aggregation.post;

import io.druid.query.aggregation.CountAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class FieldAccessPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    FieldAccessPostAggregator fieldAccessPostAggregator;

    fieldAccessPostAggregator = new FieldAccessPostAggregator("To be, or not to be, that is the question:", "rows");
    CountAggregator agg = new CountAggregator("rows");
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put(agg.getName(), agg.get());
    Assert.assertEquals(new Long(0L), fieldAccessPostAggregator.compute(metricValues));

    agg.aggregate();
    agg.aggregate();
    agg.aggregate();
    metricValues.put(agg.getName(), agg.get());
    Assert.assertEquals(new Long(3L), fieldAccessPostAggregator.compute(metricValues));
  }
}
