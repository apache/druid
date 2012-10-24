package com.metamx.druid.aggregation;

import com.google.common.primitives.Longs;
import com.metamx.druid.processing.FloatMetricSelector;

import java.util.Comparator;

/**
 */
public class LongSumAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((Number) o).longValue(), ((Number) o1).longValue());
    }
  };

  static long combineValues(Object lhs, Object rhs) {
    return ((Number) lhs).longValue() + ((Number) rhs).longValue();
  }

  private final FloatMetricSelector selector;
  private final String name;

  private long sum;

  public LongSumAggregator(String name, FloatMetricSelector selector)
  {
    this.name = name;
    this.selector = selector;

    this.sum = 0;
  }

  @Override
  public void aggregate()
  {
    sum += (long) selector.get();
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
    return name;
  }

  @Override
  public Aggregator clone()
  {
    return new LongSumAggregator(name, selector);
  }
}
