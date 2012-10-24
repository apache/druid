package com.metamx.druid.aggregation;

import com.metamx.druid.processing.FloatMetricSelector;

import java.nio.ByteBuffer;

/**
 */
public class MinBufferAggregator implements BufferAggregator
{
  private final FloatMetricSelector selector;

  public MinBufferAggregator(FloatMetricSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, Double.POSITIVE_INFINITY);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putDouble(position, Math.min(buf.getDouble(position), (double) selector.get()));
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
