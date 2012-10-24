package com.metamx.druid.aggregation;

import com.metamx.druid.processing.MetricSelectorFactory;

import java.util.Comparator;
import java.util.List;

/**
 */
public class ToLowerCaseAggregatorFactory implements AggregatorFactory
{
  private final AggregatorFactory baseAggregatorFactory;

  public ToLowerCaseAggregatorFactory(AggregatorFactory baseAggregatorFactory)
  {
    this.baseAggregatorFactory = baseAggregatorFactory;
  }

  @Override
  public Aggregator factorize(MetricSelectorFactory metricFactory)
  {
    return baseAggregatorFactory.factorize(metricFactory);
  }

  @Override
  public BufferAggregator factorizeBuffered(MetricSelectorFactory metricFactory)
  {
    return baseAggregatorFactory.factorizeBuffered(metricFactory);
  }

  @Override
  public Comparator getComparator()
  {
    return baseAggregatorFactory.getComparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return baseAggregatorFactory.combine(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return baseAggregatorFactory.getCombiningFactory();
  }

  @Override
  public Object deserialize(Object object)
  {
    return baseAggregatorFactory.deserialize(object);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return baseAggregatorFactory.finalizeComputation(object);
  }

  @Override
  public String getName()
  {
    return baseAggregatorFactory.getName().toLowerCase();
  }

  @Override
  public List<String> requiredFields()
  {
    return baseAggregatorFactory.requiredFields();
  }

  @Override
  public byte[] getCacheKey()
  {
    return baseAggregatorFactory.getCacheKey();
  }

  @Override
  public String getTypeName()
  {
    return baseAggregatorFactory.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return baseAggregatorFactory.getMaxIntermediateSize();
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return baseAggregatorFactory.getAggregatorStartValue();
  }
}
