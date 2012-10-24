package com.metamx.druid.aggregation;

import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class DoubleSumAggregatorTest
{
  private void aggregate(TestFloatMetricSelector selector, DoubleSumAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    final float[] values = {0.15f, 0.27f};
    final TestFloatMetricSelector selector = new TestFloatMetricSelector(values);
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
    final TestFloatMetricSelector selector = new TestFloatMetricSelector(new float[]{0.15f, 0.27f});
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
