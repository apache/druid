package org.apache.druid.query.aggregation.any;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

public class DoubleAnyAggregator implements Aggregator
{
  private final BaseDoubleColumnValueSelector valueSelector;

  private Double foundValue;

  public DoubleAnyAggregator(BaseDoubleColumnValueSelector valueSelector)
  {
    this.valueSelector = valueSelector;
    foundValue = null;
  }

  @Override
  public void aggregate()
  {
    if (foundValue == null && !valueSelector.isNull()) {
      foundValue = valueSelector.getDouble();
    }
  }

  @Override
  public Object get()
  {
    return foundValue;
  }

  @Override
  public float getFloat()
  {
    return foundValue.floatValue();
  }

  @Override
  public long getLong()
  {
    return foundValue.longValue();
  }

  @Override
  public double getDouble()
  {
    return foundValue;
  }

  @Override
  public void close()
  {

  }
}
