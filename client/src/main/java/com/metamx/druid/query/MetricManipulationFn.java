package com.metamx.druid.query;

import com.metamx.druid.aggregation.AggregatorFactory;

/**
*/
public interface MetricManipulationFn
{
  public Object manipulate(AggregatorFactory factory, Object object);
}
