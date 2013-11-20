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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class JavaScriptPostAggregatorTest
{
  @Test
    public void testCompute()
    {
      JavaScriptPostAggregator javaScriptPostAggregator;

      Map<String, Object> metricValues = Maps.newHashMap();
      metricValues.put("delta", -10.0);
      metricValues.put("total", 100.0);


      String absPercentFunction = "function(delta, total) { return 100 * Math.abs(delta) / total; }";
      javaScriptPostAggregator = new JavaScriptPostAggregator("absPercent", Lists.newArrayList("delta", "total"), absPercentFunction);

      Assert.assertEquals(10.0, javaScriptPostAggregator.compute(metricValues));
    }
}
