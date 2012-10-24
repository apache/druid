package com.metamx.druid.aggregation;

import com.metamx.druid.processing.FloatMetricSelector;

import java.util.Comparator;

/**
 */
public class MinAggregator implements Aggregator
{
  static final Comparator COMPARATOR = DoubleSumAggregator.COMPARATOR;

  static double combineValues(Object lhs, Object rhs)
  {
    return Math.min(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  private final FloatMetricSelector selector;
  private final String name;

  private double min;

  public MinAggregator(String name, FloatMetricSelector selector)
  {
    this.name = name;
    this.selector = selector;

    reset();
  }

  @Override
  public void aggregate()
  {
    min = Math.min(min, (double) selector.get());
  }

  @Override
  public void reset()
  {
    min = Double.POSITIVE_INFINITY;
  }

  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public float getFloat()
  {
    return (float) min;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new MinAggregator(name, selector);
  }
}
