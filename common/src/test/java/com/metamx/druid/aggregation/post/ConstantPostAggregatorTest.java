package com.metamx.druid.aggregation.post;

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
