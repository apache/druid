package com.metamx.druid.query.filter;

import java.nio.ByteBuffer;

/**
 */
public class NoopDimFilter implements DimFilter
{
  private static final byte CACHE_TYPE_ID = -0x4;

  @Override
  public byte[] getCacheKey()
  {        
    return ByteBuffer.allocate(1).put(CACHE_TYPE_ID).array();
  }
}
