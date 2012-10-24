package com.metamx.druid.aggregation;

import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class LongSumAggregatorTest
{
  private void aggregate(TestFloatMetricSelector selector, LongSumAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    final TestFloatMetricSelector selector = new TestFloatMetricSelector(new float[]{24.15f, 20f});
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
    final TestFloatMetricSelector selector = new TestFloatMetricSelector(new float[]{18293f});
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
