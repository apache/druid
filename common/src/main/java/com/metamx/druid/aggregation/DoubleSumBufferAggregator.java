package com.metamx.druid.aggregation;

import com.metamx.druid.processing.FloatMetricSelector;

import java.nio.ByteBuffer;

/**
 */
public class DoubleSumBufferAggregator implements BufferAggregator
{
  private final FloatMetricSelector selector;

  public DoubleSumBufferAggregator(
      FloatMetricSelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, 0.0d);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putDouble(position, buf.getDouble(position) + (double) selector.get());
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position);
  }
}
