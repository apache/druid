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

import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class ConstantPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    ConstantPostAggregator constantPostAggregator;

    constantPostAggregator = new ConstantPostAggregator("shichi", 7, null);
    Assert.assertEquals(7, constantPostAggregator.compute(null));
    constantPostAggregator = new ConstantPostAggregator("rei", 0.0, null);
    Assert.assertEquals(0.0, constantPostAggregator.compute(null));
    constantPostAggregator = new ConstantPostAggregator("ichi", 1.0, null);
    Assert.assertNotSame(1, constantPostAggregator.compute(null));
  }

  @Test
  public void testComparator()
  {
    ConstantPostAggregator constantPostAggregator =
        new ConstantPostAggregator("thistestbasicallydoesnothing unhappyface", 1, null);
    Comparator comp = constantPostAggregator.getComparator();
    Assert.assertEquals(0, comp.compare(0, constantPostAggregator.compute(null)));
    Assert.assertEquals(0, comp.compare(0, 1));
    Assert.assertEquals(0, comp.compare(1, 0));
  }

  @Test
  public void testSerdeBackwardsCompatible() throws Exception
  {

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ConstantPostAggregator aggregator = mapper.readValue(
        "{\"type\":\"constant\",\"name\":\"thistestbasicallydoesnothing unhappyface\",\"constantValue\":1}\n",
        ConstantPostAggregator.class
    );
    Assert.assertEquals(new Integer(1), aggregator.getConstantValue());
  }

  @Test
  public void testSerde() throws Exception
  {

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ConstantPostAggregator aggregator = new ConstantPostAggregator("aggregator", 2, null);
    ConstantPostAggregator aggregator1 = mapper.readValue(
        mapper.writeValueAsString(aggregator),
        ConstantPostAggregator.class
    );
    Assert.assertEquals(aggregator, aggregator1);
  }
}
