package org.apache.druid.query.aggregation.any;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

public class FloatAnyAggregator implements Aggregator
{
  private final BaseFloatColumnValueSelector valueSelector;

  private Float foundValue;

  public FloatAnyAggregator(BaseFloatColumnValueSelector valueSelector)
  {
    this.valueSelector = valueSelector;
    foundValue = null;
  }

  @Override
  public void aggregate()
  {
    if (foundValue == null && !valueSelector.isNull()) {
      foundValue = valueSelector.getFloat();
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
    return foundValue;
  }

  @Override
  public long getLong()
  {
    return foundValue.longValue();
  }

  @Override
  public double getDouble()
  {
    return foundValue.doubleValue();
  }

  @Override
  public void close()
  {

  }
}
