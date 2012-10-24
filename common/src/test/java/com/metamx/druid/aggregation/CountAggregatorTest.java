package com.metamx.druid.aggregation;

import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class CountAggregatorTest
{
  @Test
  public void testAggregate()
  {
    CountAggregator agg = new CountAggregator("billy");

    Assert.assertEquals("billy", agg.getName());

    Assert.assertEquals(0l, agg.get());
    Assert.assertEquals(0l, agg.get());
    Assert.assertEquals(0l, agg.get());
    agg.aggregate();
    Assert.assertEquals(1l, agg.get());
    Assert.assertEquals(1l, agg.get());
    Assert.assertEquals(1l, agg.get());
    agg.aggregate();
    Assert.assertEquals(2l, agg.get());
    Assert.assertEquals(2l, agg.get());
    Assert.assertEquals(2l, agg.get());
  }

  @Test
  public void testComparator()
  {
    CountAggregator agg = new CountAggregator("billy");

    Object first = agg.get();
    agg.aggregate();

    Comparator comp = new CountAggregatorFactory("null").getComparator();

    Assert.assertEquals(-1, comp.compare(first, agg.get()));
    Assert.assertEquals(0, comp.compare(first, first));
    Assert.assertEquals(0, comp.compare(agg.get(), agg.get()));
    Assert.assertEquals(1, comp.compare(agg.get(), first));
  }
}
