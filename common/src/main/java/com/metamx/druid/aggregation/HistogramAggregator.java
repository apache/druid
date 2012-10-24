package com.metamx.druid.aggregation;

import com.google.common.primitives.Longs;
import com.metamx.druid.processing.FloatMetricSelector;

import java.util.Comparator;

public class HistogramAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((Histogram) o).count, ((Histogram) o1).count);
    }
  };

  static Object combineHistograms(Object lhs, Object rhs) {
    return ((Histogram) lhs).fold((Histogram) rhs);
  }

  private final FloatMetricSelector selector;
  private final String name;

  private Histogram histogram;


  public HistogramAggregator(String name, FloatMetricSelector selector, float[] breaks) {
    this.name = name;
    this.selector = selector;
    this.histogram = new Histogram(breaks);
  }

  @Override
  public void aggregate()
  {
    histogram.offer(selector.get());
  }

  @Override
  public void reset()
  {
    this.histogram = new Histogram(histogram.breaks);
  }

  @Override
  public Object get()
  {
    return this.histogram;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("HistogramAggregator does not support getFloat()");
  }

  @Override
  public String getName()
  {
    return name;
  }
}
