package com.metamx.druid.aggregation;

import com.metamx.druid.processing.FloatMetricSelector;

import java.util.Comparator;

/**
 */
public class MaxAggregator implements Aggregator
{
  static final Comparator COMPARATOR = DoubleSumAggregator.COMPARATOR;

  static double combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  private final FloatMetricSelector selector;
  private final String name;

  private double max;

  public MaxAggregator(String name, FloatMetricSelector selector)
  {
    this.name = name;
    this.selector = selector;

    reset();
  }

  @Override
  public void aggregate()
  {
    max = Math.max(max, selector.get());
  }

  @Override
  public void reset()
  {
    max = Double.NEGATIVE_INFINITY;
  }

  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public float getFloat()
  {
    return (float) max;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new MaxAggregator(name, selector);
  }
}
