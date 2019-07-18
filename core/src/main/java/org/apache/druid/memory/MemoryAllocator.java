package org.apache.druid.memory;

import java.nio.ByteBuffer;

public interface MemoryAllocator
{
  BufferHolder allocate(int capacity);
  void free(BufferHolder bh);
}
