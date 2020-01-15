package org.apache.druid.query.aggregation.any;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.first.StringAggregatorUtils;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

public class StringAnyAggregator implements Aggregator
{
  private final BaseObjectColumnValueSelector valueSelector;
  private final int maxStringBytes;

  private String foundValue;

  public StringAnyAggregator(BaseObjectColumnValueSelector valueSelector, int maxStringBytes)
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    foundValue = null;
  }

  @Override
  public void aggregate()
  {
    if (foundValue == null) {
      final Object object = valueSelector.getObject();
      if (object != null) {
        foundValue = DimensionHandlerUtils.convertObjectToString(object);
        if (foundValue != null && foundValue.length() > maxStringBytes) {
          foundValue = foundValue.substring(0, maxStringBytes);
        }
      }
    }
  }

  @Override
  public Object get()
  {
    return StringAggregatorUtils.chop(foundValue, maxStringBytes);
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("StringAnyAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("StringAnyAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("StringAnyAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {

  }
}
