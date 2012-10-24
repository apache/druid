package com.metamx.druid.aggregation;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class MaxAggregatorTest
{
  private void aggregate(TestFloatMetricSelector selector, MaxAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate() throws Exception
  {
    final float[] values = {0.15f, 0.27f, 0.0f, 0.93f};
    final TestFloatMetricSelector selector = new TestFloatMetricSelector(values);
    MaxAggregator agg = new MaxAggregator("billy", selector);

    Assert.assertEquals("billy", agg.getName());

    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);

    Assert.assertEquals(new Float(values[3]).doubleValue(), agg.get());
  }
}
