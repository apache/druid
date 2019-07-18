package org.apache.druid.memory;

import com.google.common.base.Suppliers;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class SimpleOnHeapMemoryAllocator implements MemoryAllocator
{
  @Override
  public BufferHolder allocate(int capacity)
  {
    return new SimplerBufferHolder(ByteBuffer.allocate(capacity));
  }

  @Override
  public void free(BufferHolder ignored)
  {

  }

  private static class SimplerBufferHolder implements BufferHolder
  {
    private final ByteBuffer bb;

    public SimplerBufferHolder(ByteBuffer bb)
    {
      this.bb = bb;
    }

    @Override
    public int position()
    {
      return 0;
    }

    @Override
    public int capacity()
    {
      return bb.capacity();
    }

    @Override
    public ByteBuffer get()
    {
      return bb;
    }
  }
}
