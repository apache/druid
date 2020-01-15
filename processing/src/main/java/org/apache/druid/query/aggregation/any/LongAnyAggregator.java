package org.apache.druid.query.aggregation.any;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;

public class LongAnyAggregator implements Aggregator
{
  private final BaseLongColumnValueSelector valueSelector;

  private Long foundValue;

  public LongAnyAggregator(BaseLongColumnValueSelector valueSelector)
  {
    this.valueSelector = valueSelector;
    foundValue = null;
  }

  @Override
  public void aggregate()
  {
    if (foundValue == null && !valueSelector.isNull()) {
      foundValue = valueSelector.getLong();
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
    return foundValue;
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
