package org.apache.druid.query.aggregation.any;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.first.StringAggregatorUtils;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

import java.nio.ByteBuffer;

public class StringAnyBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector valueSelector;
  private final int maxStringBytes;

  private boolean isValueFound;

  public StringAnyBufferAggregator(BaseObjectColumnValueSelector valueSelector, int maxStringBytes)
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    isValueFound = false;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (!isValueFound) {
      final Object object = valueSelector.getObject();
      if (object != null) {
        String foundValue = DimensionHandlerUtils.convertObjectToString(object);
        if (foundValue != null) {
          ByteBuffer mutationBuffer = buf.duplicate();
          mutationBuffer.limit(maxStringBytes);
          final int len = StringUtils.toUtf8WithLimit(foundValue, mutationBuffer);
          mutationBuffer.putInt(position, len);
          isValueFound = true;
        }
      }
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer copyBuffer = buf.duplicate();
    copyBuffer.position(position);
    int stringSizeBytes = copyBuffer.getInt();
    if (stringSizeBytes >= 0) {
      byte[] valueBytes = new byte[stringSizeBytes];
      copyBuffer.get(valueBytes, 0, stringSizeBytes);
      return StringAggregatorUtils.chop(StringUtils.fromUtf8(valueBytes), maxStringBytes);
    } else {
      return null;
    }
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringAnyBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringAnyBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringAnyBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {

  }
}
