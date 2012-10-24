package com.metamx.druid.aggregation;

import com.metamx.druid.processing.FloatMetricSelector;

import java.util.Comparator;

/**
 */
public class DoubleSumAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return ((Double) o).compareTo((Double) o1);
    }
  };

  static double combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
  }

  private final FloatMetricSelector selector;
  private final String name;

  private double sum;

  public DoubleSumAggregator(String name, FloatMetricSelector selector)
  {
    this.name = name;
    this.selector = selector;

    this.sum = 0;
  }

  @Override
  public void aggregate()
  {
    sum += selector.get();
  }

  @Override
  public void reset()
  {
    sum = 0;
  }

  @Override
  public Object get()
  {
    return sum;
  }

  @Override
  public float getFloat()
  {
    return (float) sum;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new DoubleSumAggregator(name, selector);
  }
}
