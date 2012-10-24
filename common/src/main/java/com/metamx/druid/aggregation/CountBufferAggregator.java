package com.metamx.druid.aggregation;

import java.nio.ByteBuffer;

/**
 */
public class CountBufferAggregator implements BufferAggregator
{

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, 0l);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putLong(position, buf.getLong(position) + 1);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }
}
