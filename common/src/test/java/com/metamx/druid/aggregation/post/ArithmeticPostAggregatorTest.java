package com.metamx.druid.aggregation.post;

import com.google.common.collect.Lists;
import com.metamx.druid.aggregation.CountAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class ArithmeticPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    ArithmeticPostAggregator arithmeticPostAggregator;
    CountAggregator agg = new CountAggregator("rows");
    agg.aggregate();
    agg.aggregate();
    agg.aggregate();
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put(agg.getName(), agg.get());

    List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            new ConstantPostAggregator(
                "roku", 6
            ),
            new FieldAccessPostAggregator(
                "rows", "rows"
            )
        );

    arithmeticPostAggregator = new ArithmeticPostAggregator("add", "+", postAggregatorList);
    Assert.assertEquals(9.0, arithmeticPostAggregator.compute(metricValues));

    arithmeticPostAggregator = new ArithmeticPostAggregator("subtract", "-", postAggregatorList);
    Assert.assertEquals(3.0, arithmeticPostAggregator.compute(metricValues));

    arithmeticPostAggregator = new ArithmeticPostAggregator("multiply", "*", postAggregatorList);
    Assert.assertEquals(18.0, arithmeticPostAggregator.compute(metricValues));

    arithmeticPostAggregator = new ArithmeticPostAggregator("divide", "/", postAggregatorList);
    Assert.assertEquals(2.0, arithmeticPostAggregator.compute(metricValues));
  }

  @Test
  public void testComparator()
  {
    ArithmeticPostAggregator arithmeticPostAggregator;
    CountAggregator agg = new CountAggregator("rows");
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put(agg.getName(), agg.get());

    List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            new ConstantPostAggregator(
                "roku", 6
            ),
            new FieldAccessPostAggregator(
                "rows", "rows"
            )
        );

    arithmeticPostAggregator = new ArithmeticPostAggregator("add", "+", postAggregatorList);
    Comparator comp = arithmeticPostAggregator.getComparator();
    Object before = arithmeticPostAggregator.compute(metricValues);
    agg.aggregate();
    agg.aggregate();
    agg.aggregate();
    metricValues.put(agg.getName(), agg.get());
    Object after = arithmeticPostAggregator.compute(metricValues);

    Assert.assertEquals(-1, comp.compare(before, after));
    Assert.assertEquals(0, comp.compare(before, before));
    Assert.assertEquals(0, comp.compare(after, after));
    Assert.assertEquals(1, comp.compare(after, before));
  }
}