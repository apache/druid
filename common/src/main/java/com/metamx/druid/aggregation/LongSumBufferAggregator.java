package com.metamx.druid.aggregation;

import com.metamx.druid.processing.FloatMetricSelector;

import java.nio.ByteBuffer;

/**
 */
public class LongSumBufferAggregator implements BufferAggregator
{
  private final FloatMetricSelector selector;

  public LongSumBufferAggregator(
      FloatMetricSelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, 0l);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putLong(position, buf.getLong(position) + (long) selector.get());
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getLong(position);
  }
}
