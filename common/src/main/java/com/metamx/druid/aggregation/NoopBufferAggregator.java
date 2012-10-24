package com.metamx.druid.aggregation;

import java.nio.ByteBuffer;

/**
 */
public class NoopBufferAggregator implements BufferAggregator
{
  @Override
  public void init(ByteBuffer buf, int position)
  {
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return null;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return 0;
  }
}
