package com.metamx.druid.aggregation.post;

import com.metamx.druid.aggregation.CountAggregator;
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
